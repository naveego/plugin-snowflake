using System.Text;
using System.Threading.Tasks;
using Aunalytics.Sdk.Logging;
using PluginSnowflake.API.Factory;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        private static readonly string EnsureTableQuery = @"SELECT COUNT(*) as C
FROM information_schema.tables 
WHERE table_schema = '{0}' 
AND table_name = '{1}'";

        // private static readonly string EnsureTableQuery = @"SELECT * FROM {0}.{1}";

        public static async Task EnsureTableAsync(IConnectionFactory connFactory, ReplicationTable table)
        {
            var conn = connFactory.GetConnection();

            try
            {
                ICommand cmd;
                
                await conn.OpenAsync();

                if (table.ShouldCreateSchema)
                {
                    Logger.Info($"Creating Schema... {table.SchemaName}");
                
                    cmd = connFactory.GetCommand($"CREATE SCHEMA IF NOT EXISTS {Utility.Utility.GetSafeName(table.SchemaName)}", conn);
                    await cmd.ExecuteNonQueryAsync();
                }

                cmd = connFactory.GetCommand(string.Format(EnsureTableQuery, table.SchemaName, table.TableName), conn);

                Logger.Info($"Creating Table: {string.Format(EnsureTableQuery, table.SchemaName, table.TableName)}");

                // check if table exists
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var count = (long) reader.GetValueById("C");
                await conn.CloseAsync();

                if (count == 0)
                {
                    // create table
                    var querySb = new StringBuilder($@"CREATE TABLE IF NOT EXISTS 
{Utility.Utility.GetSafeName(table.SchemaName)}.{Utility.Utility.GetSafeName(table.TableName)}(");
                    var primaryKeySb = new StringBuilder("PRIMARY KEY (");
                    var hasPrimaryKey = false;
                    foreach (var column in table.Columns)
                    {
                        querySb.Append(
                            $"{Utility.Utility.GetSafeName(column.ColumnName)} {column.DataType}{(column.PrimaryKey ? " NOT NULL UNIQUE" : "")},");
                        if (column.PrimaryKey)
                        {
                            primaryKeySb.Append($"{Utility.Utility.GetSafeName(column.ColumnName)},");
                            hasPrimaryKey = true;
                        }
                    }

                    if (hasPrimaryKey)
                    {
                        primaryKeySb.Length--;
                        primaryKeySb.Append(")");
                        querySb.Append($"{primaryKeySb});");
                    }
                    else
                    {
                        querySb.Length--;
                        querySb.Append(");");
                    }

                    var query = querySb.ToString();
                    Logger.Info($"Creating Table: {query}");

                    await conn.OpenAsync();

                    cmd = connFactory.GetCommand(query, conn);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}