/*
 * Copyright 2013-2026 Erudika. https://erudika.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 * For issues and patches go to: https://github.com/erudika
 */
package com.erudika.para.server.queue;

import com.erudika.para.core.App;
import com.erudika.para.core.ParaObject;
import com.erudika.para.core.listeners.IOListener;
import com.erudika.para.core.utils.Config;
import com.erudika.para.core.utils.Para;
import com.erudika.para.core.utils.ParaObjectUtils;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.Strings;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.regions.providers.DefaultAwsRegionProviderChain;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchRequestEntry;
import software.amazon.awssdk.services.sqs.model.SendMessageBatchResponse;

/**
 * Listens for IO events and forwards them to a queue for indexing in other regions.
 * This keeps the search indices in all regions in sync.
 * @author Alex Bogdanovski [alex@erudika.com]
 */
public class GlobalIndexingIOListener implements IOListener {

	private static final Logger logger = LoggerFactory.getLogger(GlobalIndexingIOListener.class);
	private static final Map<String, SqsClient> SQS_CLIENTS = new HashMap<>(5);
	private static final Map<String, String> SQS_URLS = new HashMap<>(5);
	private static final ConcurrentLinkedQueue<String> PENDING_MESSAGES;
	private static final int MAX_PENDING_MESSAGES = 10;

	static {
		PENDING_MESSAGES = new ConcurrentLinkedQueue<>();
		try {
			for (String region : Para.getConfig().replicaRegions()) {
				SQS_CLIENTS.put(region, SqsClient.builder().region(Region.of(region)).build());
				SQS_URLS.put(region, SQS_CLIENTS.get(region).
						getQueueUrl(b -> b.queueName(Para.getConfig().defaultQueueName())).queueUrl());
			}
		} catch (Exception e) {
			logger.error(null, e);
		}
		if (AWSQueue.class.getSimpleName().equals(Para.getConfig().queuePlugin())) {
			App.addAppCreatedListener((App app) -> {
				if (!app.isRootApp()) {
					sendIndexPayloadToOtherRegions(Collections.
							singletonList(buildPayloadAsJSON("create_index_op", app.getAppIdentifier(), app)));
				}
			});
			App.addAppDeletedListener((App app) -> {
				if (!app.isRootApp()) {
					sendIndexPayloadToOtherRegions(Collections.
							singletonList(buildPayloadAsJSON("delete_index_op", app.getAppIdentifier(), app)));
				}
			});
			Para.asyncExecutePeriodically(() -> sendMessagesInBatch(), 0,
					Para.getConfig().globalSyncIntervalSec(), TimeUnit.SECONDS);
		}
	}

	public GlobalIndexingIOListener() {	}

	@Override
	public void onPreInvoke(Method method, Object[] args) {
		// noop
	}

	@Override
	@SuppressWarnings("unchecked")
	public void onPostInvoke(Method method, Object[] args, Object result) {
		if (method != null && !method.getName().startsWith("read")) {
			final String opId;
			opId = switch (method.getName()) {
				case "create", "createAll", "update", "updateAll" -> "index_all_op";
				case "delete", "deleteAll" -> "unindex_all_op";
				default -> "";
			};
			if (!StringUtils.isBlank(opId)) {
				String appid = args.length > 0 && args[0] instanceof String ? (String) args[0] :
						Para.getConfig().getRootAppIdentifier();
				PENDING_MESSAGES.add(buildPayloadAsJSON(opId, appid, getObjectsFromArguments(args)));
				if (PENDING_MESSAGES.size() >= MAX_PENDING_MESSAGES) {
					sendMessagesInBatch();
				}
			}
		}
	}

	public static void triggerReindexInOtherRegions(App app) {
		if (app != null) {
			sendIndexPayloadToOtherRegions(Collections.
					singletonList(buildPayloadAsJSON("rebuild_index_op", app.getAppIdentifier(), app)));
		}
	}

	private static void sendIndexPayloadToOtherRegions(Collection<String> messages) {
		if (!Para.getConfig().replicaRegions().isEmpty() && messages != null && !messages.isEmpty()) {
			try {
				String awsRegion = new DefaultAwsRegionProviderChain().getRegion().id();
				SQS_CLIENTS.keySet().stream().filter(k -> !k.equals(awsRegion)).forEach(region -> {
					logger.debug("Sending {} indexing messages to queue in region '" + region + "'.", messages.size());
					List<SendMessageBatchRequestEntry> msgs = messages.stream().filter(m -> !StringUtils.isBlank(m)).
							map(m -> SendMessageBatchRequestEntry.builder().id(UUID.randomUUID().toString()).
									messageBody(m).build()).collect(Collectors.toList());
					if (msgs.isEmpty()) {
						return;
					}
					SendMessageBatchResponse res = SQS_CLIENTS.get(region).
							sendMessageBatch(b -> b.queueUrl(SQS_URLS.get(region)).entries(msgs));
					if (res != null && res.hasFailed()) {
						logger.error("{} indexing messages not delivered to queue in region '{}': {} {}", res.failed().size(),
								region, res.sdkHttpResponse().statusCode(), res.sdkHttpResponse().statusText().orElse(""));
					} else {
						logger.debug("Sent {} indexing messages in batch to queue.", messages.size());
					}
				});
			} catch (Exception e) {
				logger.error(null, e);
			}
		}
	}

	private static void sendMessagesInBatch() {
		if (PENDING_MESSAGES.isEmpty()) {
			return;
		}
		logger.debug("{} indexing messages waiting to be sent to queue.", PENDING_MESSAGES.size());
		List<String> batch = new ArrayList<>(MAX_PENDING_MESSAGES);
		String message;
		while ((message = PENDING_MESSAGES.poll()) != null) {
			batch.add(message);
			if (batch.size() == MAX_PENDING_MESSAGES) {
				sendIndexPayloadToOtherRegions(batch);
				batch.clear();
			}
		}
		if (!batch.isEmpty()) {
			sendIndexPayloadToOtherRegions(batch);
		}
	}

	@SuppressWarnings("unchecked")
	private static String buildPayloadAsJSON(String opId, String appid, Object payload) {
		if (payload == null) {
			logger.debug("Empty payload in queue message: op {} appid {}", opId, appid);
			return "";
		}
		Map<String, Object> data = new HashMap<>();
		data.put(Config._ID, opId);
		data.put(Config._APPID, appid);
		data.put(Config._TYPE, "indexpayload");
		try {
			if (payload instanceof List) {
				data.put("payload", ((List<ParaObject>) payload).stream().map(po -> po.getId()).
						collect(Collectors.toList()));
			} else if (Strings.CI.equalsAny(opId, "rebuild_index_op", "create_index_op", "delete_index_op")) {
				data.put("payload", payload); // payload is the app object
			} else {
				data.put("payload", Collections.singletonList(((ParaObject) payload).getId()));
			}
			return ParaObjectUtils.getJsonWriterNoIdent().writeValueAsString(data);
		} catch (Exception e) {
			logger.error(null, e);
		}
		return "";
	}

	private Object getObjectsFromArguments(Object[] args) {
		for (Object arg : args) {
			if (arg != null && arg instanceof ParaObject) {
				return (ParaObject) arg;
			}
		}
		for (Object arg : args) {
			if (arg != null && arg instanceof List) {
				List<?> list = (List) arg;
				if (!list.isEmpty() && list.get(0) instanceof ParaObject) {
					return list;
				}
			}
		}
		return null;
	}
}
