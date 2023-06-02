using System;
using System.Collections.Generic;
using Aunalytics.Sdk.Logging;
using Aunalytics.Sdk.Plugins;
using Newtonsoft.Json;
using PluginSnowflake.API.Factory;

namespace PluginSnowflake.API.Read
{
    public static partial class Read
    {
        public static async IAsyncEnumerable<Record> ReadRecords(IConnectionFactory connFactory, Schema schema)
        {
            var conn = connFactory.GetConnection();
            
            try
            {
                await conn.OpenAsync();

                var query = schema.Query;

                if (string.IsNullOrWhiteSpace(query))
                {
                    query = $"SELECT * FROM {schema.Id}";
                }

                var cmd = connFactory.GetCommand(query, conn);
                IReader reader;

                try
                {
                    reader = await cmd.ExecuteReaderAsync();
                }
                catch (Exception e)
                {
                    Logger.Error(e, e.Message);
                    yield break;
                }

                if (reader.HasRows())
                {
                    while (await reader.ReadAsync())
                    {
                        var recordMap = new Dictionary<string, object>();

                        foreach (var property in schema.Properties)
                        {
                            try
                            {
                                var rawValue = reader.GetValueById(property.Id);

                                if (rawValue is DBNull)
                                {
                                    recordMap[property.Id] = null;
                                }
                                else
                                {
                                    switch (property.Type)
                                    {
                                        case PropertyType.String:
                                        case PropertyType.Text:
                                        case PropertyType.Decimal:
                                            recordMap[property.Id] = rawValue.ToString();
                                            break;
                                        default:
                                            recordMap[property.Id] = rawValue;
                                            break;
                                    }
                                }
                            }
                            catch (Exception e)
                            {
                                Logger.Error(e, $"No column with property Id: {property.Id}");
                                Logger.Error(e, e.Message);
                                recordMap[property.Id] = null;
                            }
                        }

                        var record = new Record
                        {
                            Action = Record.Types.Action.Upsert,
                            DataJson = JsonConvert.SerializeObject(recordMap)
                        };

                        yield return record;
                    }
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}