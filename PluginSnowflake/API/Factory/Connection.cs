using System.Data;
using System.Threading.Tasks;
using PluginSnowflake.Helper;
using Snowflake.Data.Client;

namespace PluginSnowflake.API.Factory
{
    public class Connection : IConnection
    {
        private readonly SnowflakeDbConnection _conn;

        public Connection(Settings settings)
        {
            _conn = new SnowflakeDbConnection();
            _conn.ConnectionString = settings.GetConnectionString();
        }

        public Connection(Settings settings, string database)
        {
            _conn = new SnowflakeDbConnection();
            _conn.ConnectionString = settings.GetConnectionString(database);
        }

        public async Task OpenAsync()
        {
            // await _conn.OpenAsync();
            _conn.Open();
        }

        public async Task CloseAsync()
        {
            // await _conn.CloseAsync();
            _conn.Close();
        }

        public async Task<bool> PingAsync()
        {
            return _conn.IsOpen();
        }

        public IDbConnection GetConnection()
        {
            return _conn;
        }
    }
}