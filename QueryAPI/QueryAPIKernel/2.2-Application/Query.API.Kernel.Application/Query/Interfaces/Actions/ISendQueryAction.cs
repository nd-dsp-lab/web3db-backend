using Query.API.Kernel.Domain.Contracts.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Application.Query.Interfaces.Actions
{
    public interface ISendQueryAction
    {
        Task<ReturnQueryResults> SendQueryAsync(SendQueryDTO query);
    }
}
