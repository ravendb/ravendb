namespace Voron.Impl.Journal
{
    /// <summary>
    /// It's meant to be used only on list of pages that are sorted descending by page number.
    /// We need to iterate from the end in order to filter out pages that was overwritten by later transaction.
    /// </summary>
    public class RecoveryOverflowDetector
    {
        private long _minPageChecked = -1;
        private long _minPageOverlappedByAnotherPage = -1;

        public bool IsOverlappingAnotherPage(long pageNumber, int numberOfPages)
        {
            var maxPageRange = pageNumber + numberOfPages - 1;

            if (_minPageChecked != -1 && maxPageRange >= _minPageChecked)
            {
                // this page is not in use, there is a valid page having higher number which overlaps this one

                _minPageOverlappedByAnotherPage = pageNumber;
                return true;
            }

            if (_minPageOverlappedByAnotherPage != -1 && maxPageRange >= _minPageOverlappedByAnotherPage)
            {
                // this page is not in use, there is a page having higher number which overlaps this one and was modified in later transaction

                _minPageOverlappedByAnotherPage = pageNumber;
                return true;
            }

            return false;
        }

        public void SetPageChecked(long pageNumber)
        {
            _minPageChecked = pageNumber;
        }
    }
}
