using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace CqrsFramework.Infrastructure
{
    public interface ITimeProvider
    {
        DateTime Get();
        Task WaitUntil(DateTime time, CancellationToken cancel);
    }

    public class RealTimeProvider : ITimeProvider
    {
        public DateTime Get()
        {
            return DateTime.UtcNow;
        }

        public Task WaitUntil(DateTime time, CancellationToken cancel)
        {
            return Task.Delay(time - Get(), cancel);
        }
    }
}
