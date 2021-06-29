/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.trino.plugin.opengauss;

import io.trino.plugin.jdbc.*;
import io.trino.plugin.jdbc.mapping.IdentifierMapping;
import io.trino.plugin.postgresql.PostgreSqlClient;
import io.trino.plugin.postgresql.PostgreSqlConfig;
import io.trino.spi.TrinoException;
import io.trino.spi.connector.ConnectorSession;
import io.trino.spi.type.*;
import org.postgresql.util.PGobject;

import javax.inject.Inject;
import java.sql.Connection;
import java.sql.Types;
import java.util.Optional;

import static io.trino.plugin.jdbc.JdbcErrorCode.JDBC_ERROR;
import static io.trino.plugin.jdbc.StandardColumnMappings.*;
import static io.trino.plugin.opengauss.TypeUtils.getArrayElementPgTypeName;
import static io.trino.plugin.postgresql.PostgreSqlConfig.ArrayMapping.AS_ARRAY;
import static io.trino.plugin.postgresql.PostgreSqlSessionProperties.getArrayMapping;
import static io.trino.spi.type.DateTimeEncoding.unpackMillisUtc;
import static io.trino.spi.type.TimestampType.TIMESTAMP_MILLIS;
import static io.trino.spi.type.TimestampType.createTimestampType;
import static io.trino.spi.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static io.trino.spi.type.VarbinaryType.VARBINARY;

public class OpenGaussClient
        extends PostgreSqlClient {
    @Inject
    public OpenGaussClient(BaseJdbcConfig config, PostgreSqlConfig postgreSqlConfig,
                           ConnectionFactory connectionFactory, TypeManager typeManager, IdentifierMapping identifierMapping) {
        super(config, postgreSqlConfig, connectionFactory, typeManager, identifierMapping);
    }

    @Override
    public WriteMapping toWriteMapping(ConnectorSession session, Type type) {
        if (VARBINARY.equals(type)) {
            return WriteMapping.sliceMapping("bytea", varbinaryWriteFunction());
        }
        if (TIMESTAMP_MILLIS.equals(type)) {
            return WriteMapping.longMapping("timestamp", timestampWriteFunctionUsingSqlTimestamp(TIMESTAMP_MILLIS));
        }
        if (TIMESTAMP_WITH_TIME_ZONE.equals(type)) {
            return WriteMapping.longMapping("timestamp with time zone", timestampWithTimeZoneWriteFunction());
        }
        if (TinyintType.TINYINT.equals(type)) {
            return WriteMapping.longMapping("smallint", tinyintWriteFunction());
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.JSON)) {
            return WriteMapping.sliceMapping("json", typedVarcharWriteFunction("json"));
        }
        if (type.getTypeSignature().getBase().equals(StandardTypes.UUID)) {
            return WriteMapping.sliceMapping("uuid", uuidWriteFunction());
        }
        if (type instanceof ArrayType && getArrayMapping(session) == AS_ARRAY) {
            Type elementType = ((ArrayType) type).getElementType();
            String elementDataType = toWriteMapping(session, elementType).getDataType();
            return WriteMapping.objectMapping(elementDataType + "[]", arrayWriteFunction(session, elementType, getArrayElementPgTypeName(session, this, elementType)));
        }
        return super.toWriteMapping(session, type);
    }

    protected static LongWriteFunction timestampWithTimeZoneWriteFunction() {
        return (statement, index, value) -> {
            // PostgreSQL does not store zone information in "timestamp with time zone" data type
            long millisUtc = unpackMillisUtc(value);
            statement.setTimestamp(index, new java.sql.Timestamp(millisUtc));
        };
    }

    /*@Override
    public ColumnMapping jsonColumnMapping() {
        return ColumnMapping.sliceMapping(
                jsonType,
                (resultSet, columnIndex) -> jsonParse(utf8Slice(resultSet.getString(columnIndex))),
                typedVarcharWriteFunction("json"),
                DISABLE_PUSHDOWN);
    }*/

    /*@Override
    public ColumnMapping typedVarcharColumnMapping(String jdbcTypeName) {
        return ColumnMapping.sliceMapping(
                VarcharType.VARCHAR,
                (resultSet, columnIndex) -> utf8Slice(resultSet.getString(columnIndex)),
                typedVarcharWriteFunction(jdbcTypeName));
    }*/

    @Override
    public Optional<ColumnMapping> toColumnMapping(ConnectorSession session, Connection connection, JdbcTypeHandle typeHandle) {
        String jdbcTypeName = typeHandle.getJdbcTypeName()
                .orElseThrow(() -> new TrinoException(JDBC_ERROR, "Type name is missing: " + typeHandle));

        switch (jdbcTypeName) {
            case "uuid":
                return Optional.of(uuidColumnMapping());
            case "jsonb":
            case "json":
                return Optional.of(jsonColumnMapping());
            case "timestamptz":
                // OpenGauss's "timestamp with time zone" is reported as Types.TIMESTAMP rather than Types.TIMESTAMP_WITH_TIMEZONE
                int decimalDigits = typeHandle.getRequiredDecimalDigits();
                return Optional.of(timestampWithTimeZoneColumnMapping(decimalDigits));
        }
        if (typeHandle.getJdbcType() == Types.VARCHAR && !jdbcTypeName.equals("varchar")) {
            // This can be e.g. an ENUM
            return Optional.of(typedVarcharColumnMapping(jdbcTypeName));
        }
        if (typeHandle.getJdbcType() == Types.TIMESTAMP) {
            TimestampType timestampType = createTimestampType(typeHandle.getRequiredDecimalDigits());
            return Optional.of(ColumnMapping.longMapping(
                    timestampType,
                    timestampReadFunction(timestampType),
                    PostgreSqlClient::shortTimestampWriteFunction));
        }
        if (typeHandle.getJdbcType() == Types.ARRAY && getArrayMapping(session) == AS_ARRAY) {
            if (!typeHandle.getArrayDimensions().isPresent()) {
                return Optional.empty();
            }
            JdbcTypeHandle elementTypeHandle = getArrayElementTypeHandle(connection, typeHandle);
            String elementTypeName = typeHandle.getJdbcTypeName()
                    .orElseThrow(() -> new TrinoException(JDBC_ERROR,
                            "Element type name is missing: " + elementTypeHandle));
            if (elementTypeHandle.getJdbcType() == Types.VARBINARY) {
                // OpenGauss jdbc driver doesn't currently support array of varbinary (bytea[])
                // https://github.com/pgjdbc/pgjdbc/pull/1184
                return Optional.empty();
            }
            return toColumnMapping(session, connection, elementTypeHandle)
                    .map(elementMapping -> {
                        ArrayType prestoArrayType = new ArrayType(elementMapping.getType());
                        int arrayDimensions = typeHandle.getArrayDimensions().get();
                        for (int i = 1; i < arrayDimensions; i++) {
                            prestoArrayType = new ArrayType(prestoArrayType);
                        }
                        return arrayColumnMapping(session, prestoArrayType, elementMapping, elementTypeName);
                    });
        }
        return super.toColumnMapping(session, connection, typeHandle);
    }

    protected static SliceWriteFunction typedVarcharWriteFunction(String jdbcTypeName) {
        return (statement, index, value) -> {
            PGobject pgObject = new PGobject();
            pgObject.setType(jdbcTypeName);
            pgObject.setValue(value.toStringUtf8());
            statement.setObject(index, pgObject);
        };
    }
}
