using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Domain.Core.Settings
{
    public class DbSettings
    {
        public string DbProvider { get; set; }
        public bool UseSecret { get; set; }
        public string Server { get; set; }
        public string Port { get; set; }
        public string Database { get; set; }
        public string Schema { get; set; }
        public string Username { get; set; }
        public string SecretName { get; set; }
        public string Password { get; set; }

    }
}
