using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Google.Protobuf.WellKnownTypes;
using Naveego.Sdk.Plugins;
using PluginSnowflake.API.Factory;

namespace PluginSnowflake.API.Discover
{
    public static partial class Discover
    {
        private const string TableName = "TABLE_NAME";
        private const string TableSchema = "TABLE_SCHEMA";
        private const string TableType = "TABLE_TYPE";
        private const string ColumnName = "COLUMN_NAME";
        private const string DataType = "DATA_TYPE";
        private const string ColumnKey = "COLUMN_KEY";
        private const string IsNullable = "IS_NULLABLE";
        private const string CharacterMaxLength = "CHARACTER_MAXIMUM_LENGTH";

        private const string GetAllTablesAndColumnsQuery = @"
SELECT DISTINCT
    t.TABLE_SCHEMA as TABLE_SCHEMA,
    t.TABLE_NAME as TABLE_NAME,
    t.TABLE_TYPE as TABLE_TYPE,
    c.COLUMN_NAME as COLUMN_NAME,
    c.DATA_TYPE as DATA_TYPE,
    '0' as COLUMN_KEY,
    c.IS_NULLABLE as IS_NULLABLE,
    c.CHARACTER_MAXIMUM_LENGTH as CHARACTER_MAXIMUM_LENGTH,
    c.ORDINAL_POSITION
FROM INFORMATION_SCHEMA.TABLES AS t
LEFT OUTER JOIN INFORMATION_SCHEMA.COLUMNS AS c ON c.TABLE_SCHEMA = t.TABLE_SCHEMA AND c.TABLE_NAME = t.TABLE_NAME
WHERE t.TABLE_SCHEMA != 'INFORMATION_SCHEMA'
ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION";

        public static async IAsyncEnumerable<Schema> GetAllSchemas(IConnectionFactory connFactory, int sampleSize = 5)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(GetAllTablesAndColumnsQuery, conn);
                var reader = await cmd.ExecuteReaderAsync();

                Schema schema = null;
                var currentSchemaId = "";
                while (await reader.ReadAsync())
                {
                    var schemaId =
                        $"{Utility.Utility.GetSafeName(reader.GetValueById(TableSchema).ToString())}.{Utility.Utility.GetSafeName(reader.GetValueById(TableName).ToString())}";
                    if (schemaId != currentSchemaId)
                    {
                        // return previous schema
                        if (schema != null)
                        {
                            // get sample and count
                            yield return schema;
                        }

                        // start new schema
                        currentSchemaId = schemaId;
                        var parts = DecomposeSafeName(currentSchemaId).TrimEscape();
                        schema = new Schema
                        {
                            Id = currentSchemaId,
                            Name = $"{parts.Schema}.{parts.Table}",
                            Properties = { },
                            DataFlowDirection = Schema.Types.DataFlowDirection.Read,
                            Count = new Count { Kind = Count.Types.Kind.Unavailable }
                        };
                    }

                    // add column to schema
                    var property = new Property
                    {
                        Id = $"{Utility.Utility.GetSafeName(reader.GetValueById(ColumnName).ToString())}",
                        Name = reader.GetValueById(ColumnName).ToString(),
                        IsKey = reader.GetValueById(ColumnKey).ToString() == "1",
                        IsNullable = reader.GetValueById(IsNullable).ToString() == "YES",
                        Type = GetType(reader.GetValueById(DataType).ToString()),
                        TypeAtSource = GetTypeAtSource(reader.GetValueById(DataType).ToString(),
                            reader.GetValueById(CharacterMaxLength))
                    };
                    schema?.Properties.Add(property);
                }

                if (schema != null)
                {
                    // get sample and count
                    yield return schema;
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        private static async Task<Schema> AddSampleAndCount(IConnectionFactory connFactory, Schema schema,
            int sampleSize)
        {
            // add sample and count
            var records = Read.Read.ReadRecords(connFactory, schema).Take(sampleSize);
            schema.Sample.AddRange(await records.ToListAsync());
            schema.Count = await GetCountOfRecords(connFactory, schema);

            return schema;
        }

        public static PropertyType GetType(string dataType)
        {
            switch (dataType.ToLower())
            {
                case "datetime":
                case "timestamp":
                    return PropertyType.Datetime;
                case "date":
                    return PropertyType.Date;
                case "time":
                    return PropertyType.Time;
                case "integer":
                case "smallint":
                case "mediumint":
                case "int":
                case "bigint":
                    return PropertyType.Integer;
                case "decimal":
                    return PropertyType.Decimal;
                case "float":
                case "double":
                    return PropertyType.Float;
                case "boolean":
                    return PropertyType.Bool;
                case "binary":
                case "varbinary":
                    return PropertyType.Blob;
                case "char":
                case "varchar":
                case "string":
                    return PropertyType.String;
                case "text":
                    return PropertyType.Text;
                default:
                    return PropertyType.String;
            }
        }

        private static string GetTypeAtSource(string dataType, object maxLength)
        {
            return maxLength != null ? $"{dataType}({maxLength})" : dataType;
        }
    }
}