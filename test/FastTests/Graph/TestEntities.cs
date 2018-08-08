using System.Collections.Generic;

namespace FastTests.Graph
{
    public class Entity
    {
        public string Id;

        public string Name;

        public string References;
    }

    public class Dog
    {
        public string Id;
        public string Name;
        public string[] Likes;
        public string[] Dislikes;
    }

    public class Movie
    {
        public string Id { get; set; }
        public string Name { get; set; }
        public List<string> Genres { get; set; }
    }

    public class Genre
    {
        public string Id { get; set; }
        public string Name { get; set; }
    }

    public class User
    {
        public string Id { get; set; }
        public string Name { get;set; }
        public int Age { get; set; }

        public List<Rating> HasRated { get; set; }

        public class Rating
        {
            public string Movie { get; set; }
            public int Score { get; set; }
        }
    }
}
