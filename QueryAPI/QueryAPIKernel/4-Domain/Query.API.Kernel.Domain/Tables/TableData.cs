using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Domain.Tables
{
    public class TableData
    {
        public List<DatabaseRow> Rows { get; set; }
        public Dictionary<string, string> Columns { get; set; }
    }

}
