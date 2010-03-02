namespace Rhino.DivanDB.Extensions
{
    public class Tuple<T1,T2>
    {
        public Tuple(T1 first, T2 second)
        {
            First = first;
            Second = second;
        }

        public Tuple()
        {
            
        }

        public T1 First { get; set; }
        public T2 Second { get; set; }
    }


    public class Tuple<T1, T2,T3>
    {
        public Tuple(T1 first, T2 second, T3 third)
        {
            First = first;
            Second = second;
            Third = third;
        }

        public Tuple()
        {

        }

        public T1 First { get; set; }
        public T2 Second { get; set; }
        public T3 Third { get; set; }
    }
}