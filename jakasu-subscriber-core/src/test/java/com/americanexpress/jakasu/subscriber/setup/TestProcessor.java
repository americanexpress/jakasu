/*
 *
 * Copyright 2020 American Express Travel Related Services Company, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */

package com.americanexpress.jakasu.subscriber.setup;

import com.americanexpress.jakasu.subscriber.streams.StreamProcessor;
import org.apache.kafka.common.header.Headers;

import java.util.Map;

public class TestProcessor extends StreamProcessor {


    @Override
    protected String process(String payLoad, Map<String, String> headers) {
        if (isNumeric(payLoad)) {
            return null;
        }

        if (payLoad.equals("fail")) {
            int throwException = 6 / 0;
        }

        return payLoad.toUpperCase();
    }

    @Override
    protected String handleError(Exception ex, String payLoad, Map<String, String> headers) {
        return "ERROR: " + payLoad;
    }

    @Override
    protected void transformHeaders(Headers headers) {
        if (headers.toArray().length >= 1) {
            headers.add("test", "testVal".getBytes());
        }
    }

    private boolean isNumeric(String strNum) {
        return strNum.matches("-?\\d+(\\.\\d+)?");
    }
}
