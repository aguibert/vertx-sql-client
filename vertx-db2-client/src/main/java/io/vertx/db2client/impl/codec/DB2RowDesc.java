/*
 * Copyright (C) 2019,2020 IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.vertx.db2client.impl.codec;

import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import io.vertx.db2client.impl.drda.ColumnMetaData;
import io.vertx.sqlclient.impl.RowDesc;

class DB2RowDesc extends RowDesc {

    private final ColumnMetaData columnDefinitions;

    DB2RowDesc(ColumnMetaData columnDefinitions) {
        super(toList(columnDefinitions.sqlxName_));
        this.columnDefinitions = columnDefinitions;
    }

    ColumnMetaData columnDefinitions() {
        return columnDefinitions;
    }
    
    private static List<String> toList(String[] array) {
    	if (array == null || array.length == 0)
    		return Collections.emptyList();
    	else
    		return Collections.unmodifiableList(Stream.of(array).collect(Collectors.toList()));
    }

}
