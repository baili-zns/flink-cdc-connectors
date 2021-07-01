/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.alibaba.ververica.cdc.connectors.mysql.source.events;

import org.apache.flink.api.connector.source.SourceEvent;

import com.alibaba.ververica.cdc.connectors.mysql.source.enumerator.MySQLSourceEnumerator;
import com.alibaba.ververica.cdc.connectors.mysql.source.reader.MySQLSourceReader;

import java.util.List;

/**
 * The {@link SourceEvent} that {@link MySQLSourceEnumerator} sends to {@link MySQLSourceReader} to
 * notify the split has received.
 */
public class EnumeratorAckEvent implements SourceEvent {

    private static final long serialVersionUID = 1L;

    private final List<String> finishedSplits;

    public EnumeratorAckEvent(List<String> finishedSplits) {
        this.finishedSplits = finishedSplits;
    }

    public List<String> getFinishedSplits() {
        return finishedSplits;
    }
}
