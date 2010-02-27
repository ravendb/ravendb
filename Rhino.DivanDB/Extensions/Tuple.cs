namespace Rhino.DivanDB.Extensions
{
    public class Tuple<T1,T2>
    {
        public Tuple(T1 first, T2 sEcond)
        {
            First = first;
            Second = sEcond;
        }

        public Tuple()
        {
            
        }

        public T1 First { get; set; }
        public T2 Second { get; set; }
    }
}