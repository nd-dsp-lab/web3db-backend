using System;
using System.Collections.Generic;

namespace Query.API.Kernel.Domain.Contracts.DTOs
{
    public class ReturnQueryResults
    {
        public string? Message { get; set; }
        public List<Dictionary<string, object>>? Data { get; set; }
        public string? Hash { get; set; }
        public string? Error { get; set; } 
    }
}
