using Query.API.Kernel.Domain.Tables;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.DataAccess.Repositories.Interfaces
{
    public interface IDynamicSqlRepository
    {
        Task<TableData> GetFullTableAsync(string schema, string tableName);
    }
}
