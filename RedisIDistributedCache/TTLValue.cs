namespace RedisIDistributedCache
{
    public class TTLValue
    {
        public byte[] value { get; set; }
        public double slidingExpiration { get; set; }
    }
}
