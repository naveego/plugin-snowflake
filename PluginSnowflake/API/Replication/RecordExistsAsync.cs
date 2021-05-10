using System.Threading.Tasks;
using PluginSnowflake.API.Factory;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        private static readonly string RecordExistsQuery = @"SELECT COUNT(*) as C
FROM (
SELECT * FROM {0}.{1}
WHERE {2} = '{3}'    
) as q";

        public static async Task<bool> RecordExistsAsync(IConnectionFactory connFactory, ReplicationTable table,
            string primaryKeyValue)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();
            
                var cmd = connFactory.GetCommand(string.Format(RecordExistsQuery,
                        Utility.Utility.GetSafeName(table.SchemaName),
                        Utility.Utility.GetSafeName(table.TableName),
                        Utility.Utility.GetSafeName(table.Columns.Find(c => c.PrimaryKey).ColumnName),
                        primaryKeyValue
                    ),
                    conn);

                // check if record exists
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var count = (long) reader.GetValueById("C");
                
                return count != 0;
            }
            finally
            {
                await conn.CloseAsync();
            }
            
        }
    }
}