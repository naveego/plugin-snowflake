using System;
using PluginSnowflake.Helper;
using Xunit;

namespace PluginSnowflakeTest.Helper
{
    public class SettingsTest
    {
        [Fact]
        public void ValidateValidTest()
        {
            // setup
            var settings = new Settings
            {
                Account = "123.456.789.0",
                Database = "testdb",
                User = "User",
                Password = "password"
            };

            // act
            settings.Validate();

            // assert
        }

        [Fact]
        public void ValidateNoAccountTest()
        {
            // setup
            var settings = new Settings
            {
                Account = null,
                Database = "testdb",
                User = "User",
                Password = "password"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Account property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoDatabaseTest()
        {
            // setup
            var settings = new Settings
            {
                Account = "123.456.789.0",
                Database = null,
                User = "User",
                Password = "password"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Database property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoUserTest()
        {
            // setup
            var settings = new Settings
            {
                Account = "123.456.789.0",
                Database = "testdb",
                User = null,
                Password = "password"
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The User property must be set", e.Message);
        }
        
        [Fact]
        public void ValidateNoPasswordTest()
        {
            // setup
            var settings = new Settings
            {
                Account = "123.456.789.0",
                Database = "testdb",
                User = "User",
                Password = null
            };

            // act
            Exception e = Assert.Throws<Exception>(() => settings.Validate());

            // assert
            Assert.Contains("The Password property must be set", e.Message);
        }
        
        [Fact]
        public void GetConnectionStringTest()
        {
            // setup
            var settings = new Settings
            {
                Account = "123.456.789.0",
                Database = "testdb",
                User = "User",
                Password = "password"
            };

            // act
            var connString = settings.GetConnectionString();
            var connDbString = settings.GetConnectionString("otherdb");

            // assert
            Assert.Equal("account=123.456.789.0;user=User;password=password;db=testdb;", connString);
            Assert.Equal("account=123.456.789.0;user=User;password=password;db=otherdb;", connDbString);
        }
    }
}