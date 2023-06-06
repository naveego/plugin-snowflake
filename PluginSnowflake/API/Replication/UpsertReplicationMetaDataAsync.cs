using System;
using System.Threading.Tasks;
using Aunalytics.Sdk.Logging;
using Newtonsoft.Json;
using PluginSnowflake.API.Factory;
using PluginSnowflake.API.Utility;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        private static readonly string InsertMetaDataQuery = $@"INSERT INTO {{0}}.{{1}} 
(
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)}
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)})
VALUES (
'{{2}}'
, '{{3}}'
, '{{4}}'
, '{{5}}'
, '{{6}}'
)";

        private static readonly string UpdateMetaDataQuery = $@"UPDATE {{0}}.{{1}}
SET 
{Utility.Utility.GetSafeName(Constants.ReplicationMetaDataRequest)} = '{{2}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeId)} = '{{3}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataReplicatedShapeName)} = '{{4}}'
, {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataTimestamp)} = '{{5}}'
WHERE {Utility.Utility.GetSafeName(Constants.ReplicationMetaDataJobId)} = '{{6}}'";

        public static async Task UpsertReplicationMetaDataAsync(IConnectionFactory connFactory, ReplicationTable table,
            ReplicationMetaData metaData)
        {
            var conn = connFactory.GetConnection();

            try
            {
                await conn.OpenAsync();

                if (!await RecordExistsAsync(connFactory, table, metaData.Request.DataVersions.JobId))
                {
                    // insert if not found
                    var cmd = connFactory.GetCommand(
                        string.Format(InsertMetaDataQuery,
                            Utility.Utility.GetSafeName(table.SchemaName),
                            Utility.Utility.GetSafeName(table.TableName),
                            metaData.Request.DataVersions.JobId,
                            JsonConvert.SerializeObject(metaData.Request).Replace("\\", "\\\\"),
                            metaData.ReplicatedShapeId,
                            metaData.ReplicatedShapeName,
                            metaData.Timestamp
                        ),
                        conn);

                    await cmd.ExecuteNonQueryAsync();
                }
                else
                {
                    // update if found
                    var cmd = connFactory.GetCommand(
                        string.Format(UpdateMetaDataQuery,
                            Utility.Utility.GetSafeName(table.SchemaName),
                            Utility.Utility.GetSafeName(table.TableName),
                            JsonConvert.SerializeObject(metaData.Request).Replace("\\", "\\\\"),
                            metaData.ReplicatedShapeId,
                            metaData.ReplicatedShapeName,
                            metaData.Timestamp,
                            metaData.Request.DataVersions.JobId
                        ),
                        conn);

                    await cmd.ExecuteNonQueryAsync();
                }
            }
            catch (Exception e)
            {
                Logger.Error(e, $"Error Upsert Replication Metadata: {e.Message}");
                throw;
            }
            finally
            {
                await conn.CloseAsync();
            }
        }
    }
}