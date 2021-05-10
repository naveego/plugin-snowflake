using System;
using System.Threading.Tasks;
using Naveego.Sdk.Logging;
using Naveego.Sdk.Plugins;
using Newtonsoft.Json;
using PluginSnowflake.API.Factory;
using PluginSnowflake.DataContracts;
using PluginSnowflake.Helper;
using Constants = PluginSnowflake.API.Utility.Constants;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        private static readonly string GetMetaDataQuery = @"SELECT * FROM {0}.{1} WHERE {2} = '{3}'";
        private static readonly string EnsureMetaDataQuery = @"SELECT COUNT(*) as C FROM {0}.{1} WHERE {2} = '{3}'";

        public static async Task<ReplicationMetaData> GetPreviousReplicationMetaDataAsync(
            IConnectionFactory connFactory,
            string jobId,
            ReplicationTable table)
        {
            var conn = connFactory.GetConnection();

            try
            {
                ReplicationMetaData replicationMetaData = null;

                // ensure replication metadata table
                await EnsureTableAsync(connFactory, table);

                // check if metadata exists
                await conn.OpenAsync();
                
                var cmd = connFactory.GetCommand(
                    string.Format(EnsureMetaDataQuery,
                        Utility.Utility.GetSafeName(table.SchemaName),
                        Utility.Utility.GetSafeName(table.TableName),
                        Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
                        jobId),
                    conn);
                var reader = await cmd.ExecuteReaderAsync();
                await reader.ReadAsync();
                var count = (long) reader.GetValueById("C");

                if (count > 0)
                {
                    // metadata exists
                    cmd = connFactory.GetCommand(
                        string.Format(GetMetaDataQuery,
                            Utility.Utility.GetSafeName(table.SchemaName),
                            Utility.Utility.GetSafeName(table.TableName),
                            Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId),
                            jobId),
                        conn);
                    reader = await cmd.ExecuteReaderAsync();
                    
                    await reader.ReadAsync();

                    var request = JsonConvert.DeserializeObject<PrepareWriteRequest>(
                        reader.GetValueById(Constants.ReplicationMetaDataRequest).ToString());
                    var shapeName = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeName)
                        .ToString();
                    var shapeId = reader.GetValueById(Constants.ReplicationMetaDataReplicatedShapeId)
                        .ToString();
                    var timestamp = DateTime.Parse(reader.GetValueById(Constants.ReplicationMetaDataTimestamp)
                        .ToString());

                    replicationMetaData = new ReplicationMetaData
                    {
                        Request = request,
                        ReplicatedShapeName = shapeName,
                        ReplicatedShapeId = shapeId,
                        Timestamp = timestamp
                    };
                }
                
                return replicationMetaData;
            }
            catch (Exception e)
            {
                Logger.Error(e, e.Message);
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}