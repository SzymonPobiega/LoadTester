using NServiceBus;

namespace Shared
{
    public class TestMessage : IMessage
    {
        public byte[] Data { get; set; }
    }
}