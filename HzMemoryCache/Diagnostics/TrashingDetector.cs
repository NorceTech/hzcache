namespace HzCache.Diagnostics
{
    internal class TrashingDetector(string checksum)
    {
        public int Counter { get; set; } = 0;
        public string Checksum => checksum;
    }
}