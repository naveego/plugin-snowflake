using System.Collections.Generic;
using System.Threading.Tasks;
using Aunalytics.Sdk.Plugins;
using PluginSnowflake.API.Factory;

namespace PluginSnowflake.API.Discover
{
    public static partial class Discover
    {
        private const string GetTableAndColumnsQuery = @"
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
AND t.TABLE_SCHEMA = '{0}'
AND t.TABLE_NAME = '{1}' 
ORDER BY t.TABLE_NAME, c.ORDINAL_POSITION";

        public static async Task<Schema> GetRefreshSchemaForTable(IConnectionFactory connFactory, Schema schema,
            int sampleSize = 5)
        {
            var decomposed = DecomposeSafeName(schema.Id).TrimEscape();
            var conn = string.IsNullOrWhiteSpace(decomposed.Database)
                ? connFactory.GetConnection()
                : connFactory.GetConnection(decomposed.Database);

            try
            {
                await conn.OpenAsync();

                var cmd = connFactory.GetCommand(
                    string.Format(GetTableAndColumnsQuery, decomposed.Schema, decomposed.Table), conn);
                var reader = await cmd.ExecuteReaderAsync();
                var refreshProperties = new List<Property>();

                while (await reader.ReadAsync())
                {
                    // add column to refreshProperties
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
                    refreshProperties.Add(property);
                }

                // add properties
                schema.Properties.Clear();
                schema.Properties.AddRange(refreshProperties);

            

                // get sample and count
                return await AddSampleAndCount(connFactory, schema, sampleSize);
            }
            finally
            {
                await conn.CloseAsync();
            }
        }

        private static DecomposeResponse DecomposeSafeName(string schemaId)
        {
            var response = new DecomposeResponse
            {
                Database = "",
                Schema = "",
                Table = ""
            };
            var parts = schemaId.Split('.');

            switch (parts.Length)
            {
                case 0:
                    return response;
                case 1:
                    response.Table = parts[0];
                    return response;
                case 2:
                    response.Schema = parts[0];
                    response.Table = parts[1];
                    return response;
                case 3:
                    response.Database = parts[0];
                    response.Schema = parts[1];
                    response.Table = parts[2];
                    return response;
                default:
                    return response;
            }
        }

        private static DecomposeResponse TrimEscape(this DecomposeResponse response, char escape = '"')
        {
            response.Database = response.Database.Trim(escape);
            response.Schema = response.Schema.Trim(escape);
            response.Table = response.Table.Trim(escape);

            return response;
        }
    }

    class DecomposeResponse
    {
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Table { get; set; }
    }
}