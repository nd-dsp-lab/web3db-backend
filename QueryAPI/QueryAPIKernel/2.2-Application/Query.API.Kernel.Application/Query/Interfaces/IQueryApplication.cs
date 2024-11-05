using Query.API.Kernel.Domain.Contracts.DTOs;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace Query.API.Kernel.Application.Query.Interfaces
{
    public interface IQueryApplication
    {
        Task<ReturnQueryResults> SendQueryAndGetResultsAsync(SendQueryDTO queryDto);
    }
}
