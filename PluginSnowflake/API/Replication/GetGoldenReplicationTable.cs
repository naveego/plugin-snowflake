using Naveego.Sdk.Plugins;
using PluginSnowflake.API.Utility;
using PluginSnowflake.DataContracts;

namespace PluginSnowflake.API.Replication
{
    public static partial class Replication
    {
        public static ReplicationTable GetGoldenReplicationTable(Schema schema, string safeSchemaName, string safeGoldenTableName, bool shouldCreateSchema)
        {
            var goldenTable = ConvertSchemaToReplicationTable(schema, safeSchemaName, safeGoldenTableName, shouldCreateSchema);
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationRecordId,
                DataType = "varchar(255)",
                PrimaryKey = true
            });
            goldenTable.Columns.Add(new ReplicationColumn
            {
                ColumnName = Constants.ReplicationVersionIds,
                DataType = "text",
                PrimaryKey = false,
                Serialize = true
            });

            return goldenTable;
        }
    }
}