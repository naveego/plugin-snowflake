namespace PluginSnowflake.DataContracts
{
    public class ConfigureReplicationFormData
    {
        public string SchemaName { get; set; }
        public string GoldenTableName { get; set; }
        public string VersionTableName { get; set; }
        public bool ShouldCreateSchema { get; set; } = true;
    }
}