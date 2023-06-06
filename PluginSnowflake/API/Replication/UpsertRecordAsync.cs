using System;
using System.Collections.Generic;
using System.Text;
using System.Threading.Tasks;
using Aunalytics.Sdk.Logging;
using Newtonsoft.Json;
using PluginSnowflake.API.Factory;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        public static async Task UpsertRecordAsync(IConnectionFactory connFactory,
            ReplicationTable table,
            Dictionary<string, object> recordMap)
        {
            var conn = connFactory.GetConnection();
            
            try
            {
                await conn.OpenAsync();
                
                var primaryKey = table.Columns.Find(c => c.PrimaryKey);
                var primaryValue = recordMap[primaryKey.ColumnName];
                if (primaryKey.Serialize)
                {
                    primaryValue = JsonConvert.SerializeObject(primaryValue);
                }

                if (!await RecordExistsAsync(connFactory, table, primaryValue.ToString()))
                {
                    // insert record
                    var querySb =
                        new StringBuilder(
                            $"INSERT INTO {Utility.Utility.GetSafeName(table.SchemaName)}.{Utility.Utility.GetSafeName(table.TableName)}(");
                    foreach (var column in table.Columns)
                    {
                        querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                    }

                    querySb.Length--;
                    querySb.Append(") VALUES (");

                    foreach (var column in table.Columns)
                    {
                        if (recordMap.ContainsKey(column.ColumnName))
                        {
                            var rawValue = recordMap[column.ColumnName];
                            if (column.Serialize)
                            {
                                rawValue = JsonConvert.SerializeObject(rawValue);
                            }

                            querySb.Append(rawValue != null
                                ? $"'{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "''")}',"
                                : $"NULL,");
                        }
                        else
                        {
                            querySb.Append($"NULL,");
                        }
                    }

                    querySb.Length--;
                    querySb.Append(");");

                    var query = querySb.ToString();

                    Logger.Debug($"Insert record query: {query}");

                    var cmd = connFactory.GetCommand(query, conn);

                    await cmd.ExecuteNonQueryAsync();
                }
                else
                {
                    // update record
                    var querySb =
                        new StringBuilder(
                            $"UPDATE {Utility.Utility.GetSafeName(table.SchemaName)}.{Utility.Utility.GetSafeName(table.TableName)} SET ");
                    foreach (var column in table.Columns)
                    {
                        if (!column.PrimaryKey)
                        {
                            if (recordMap.ContainsKey(column.ColumnName))
                            {
                                var rawValue = recordMap[column.ColumnName];
                                if (column.Serialize)
                                {
                                    rawValue = JsonConvert.SerializeObject(rawValue);
                                }

                                if (rawValue != null)
                                {
                                    querySb.Append(
                                        $"{Utility.Utility.GetSafeName(column.ColumnName)}='{Utility.Utility.GetSafeString(rawValue.ToString(), "'", "''")}',");
                                }
                                else
                                {
                                    querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)}=NULL,");
                                }
                            }
                            else
                            {
                                querySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)}=NULL,");
                            }
                        }
                    }

                    querySb.Length--;

                    querySb.Append($" WHERE {Utility.Utility.GetSafeName(primaryKey.ColumnName)} = '{primaryValue}'");

                    var query = querySb.ToString();

                    var cmd = connFactory.GetCommand(query, conn);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"Error Upsert Record: {e.Message}");
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}