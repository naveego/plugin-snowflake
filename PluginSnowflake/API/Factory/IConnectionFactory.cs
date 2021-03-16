using PluginSnowflake.Helper;

namespace PluginSnowflake.API.Factory
{
    public interface IConnectionFactory
    {
        void Initialize(Settings settings);
        IConnection GetConnection();
        IConnection GetConnection(string database);
        ICommand GetCommand(string commandText, IConnection conn);
    }
}