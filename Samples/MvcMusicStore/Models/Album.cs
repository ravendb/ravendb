namespace MvcMusicStore.Models
{
    public partial class Album
    {
        public string Id { get; set; }
        public string AlbumArtUrl { get; set; }
        public AlbumArtist Artist { get; set; }
        public AlbumGenre Genre { get; set; }
        public decimal Price { get; set; }
        public string Title { get; set; }
        public int CountSold { get; set; }

        public class AlbumArtist
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        public class AlbumGenre
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }
    }
}