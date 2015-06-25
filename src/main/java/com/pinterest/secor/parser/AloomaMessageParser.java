/**
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.pinterest.secor.parser;

import com.pinterest.secor.common.SecorConfig;
import com.pinterest.secor.message.Message;
import com.pinterest.secor.message.ParsedMessage;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.Date;

public class AloomaMessageParser extends MessageParser {
    private static final Logger LOG = LoggerFactory.getLogger(AloomaMessageParser.class);
    protected static final String defaultDate = "unknown";
    protected static final String defaultFormatter = "yyyy-MM-dd-HH-mm";

    public AloomaMessageParser(SecorConfig config) {
        super(config);
    }

    @Override
    public ParsedMessage parse(Message message) throws Exception {
        JSONObject jsonObject = (JSONObject) JSONValue.parse(message.getPayload());
        String inputLabel = jsonObject.get("input_label").toString();
        String[] partitions = extractPartitions(jsonObject);
        boolean extractInternal = mConfig.getExtractInternal();
        byte[] messagePayload = (extractInternal) ? jsonObject.get("message").toString().getBytes() : message.getPayload();
        return new ParsedMessage(inputLabel, message.getKafkaPartition(),
                message.getOffset(), messagePayload, partitions);
    }

    @Override
    public String[] extractPartitions(Message payload) throws Exception {
        return new String[0];
    }

    public String[] extractPartitions(JSONObject jsonObject) {
        String result[] = { defaultDate };

        if (jsonObject != null) {
            Object fieldValue = jsonObject.get(mConfig.getMessageTimestampName());
            Object inputPattern = mConfig.getMessageTimestampInputPattern();
            if (fieldValue != null && inputPattern != null) {
                try {
                    SimpleDateFormat inputFormatter = new SimpleDateFormat(inputPattern.toString());
                    SimpleDateFormat outputFormatter = new SimpleDateFormat(defaultFormatter);
                    Date dateFormat = inputFormatter.parse(fieldValue.toString());
                    result[0] = outputFormatter.format(dateFormat);
                    return result;
                } catch (Exception e) {
                    LOG.warn("Impossible to convert date = " + fieldValue.toString()
                            + " for the input pattern = " + inputPattern.toString()
                            + ". Using date default=" + result[0]);
                }
            }
        }

        return result;
    }

}
