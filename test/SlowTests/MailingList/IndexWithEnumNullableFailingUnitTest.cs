using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class IndexWithEnumNullableFailingUnitTest : RavenTestBase
    {
        private class CultureDataIndex : AbstractIndexCreationTask<CultureData>
        {
            public CultureDataIndex()
            {
                this.Map = (items => items.Select(item => new
                {
                    //item.Language,
                    LanguageCultureName = item.LanguageCultureName,
                    item.Identifier
                }));
            }
        }
        private enum LanguageCultureName
        {
            [System.ComponentModel.Description("Afrikaans - South Africa")]
            AfrikaansSouthAfrica,

            [System.ComponentModel.Description("Albanian - Albania")]
            AlbanianAlbania,

            [System.ComponentModel.Description("Arabic - Algeria")]
            ArabicAlgeria,

            [System.ComponentModel.Description("Arabic - Bahrain")]
            ArabicBahrain,

            [System.ComponentModel.Description("Arabic - Egypt")]
            ArabicEgypt,

            [System.ComponentModel.Description("Arabic - Iraq")]
            ArabicIraq,

            [System.ComponentModel.Description("Arabic - Jordan")]
            ArabicJordan,

            [System.ComponentModel.Description("Arabic - Kuwait")]
            ArabicKuwait,

            [System.ComponentModel.Description("Arabic - Lebanon")]
            ArabicLebanon,

            [System.ComponentModel.Description("Arabic - Libya")]
            ArabicLibya,

            [System.ComponentModel.Description("Arabic - Morocco")]
            ArabicMorocco,

            [System.ComponentModel.Description("Arabic - Oman")]
            ArabicOman,

            [System.ComponentModel.Description("Arabic - Qatar")]
            ArabicQatar,

            [System.ComponentModel.Description("Arabic - Saudi Arabia")]
            ArabicSaudiArabia,

            [System.ComponentModel.Description("Arabic - Syria")]
            ArabicSyria,

            [System.ComponentModel.Description("Arabic - Tunisia")]
            ArabicTunisia,

            [System.ComponentModel.Description("Arabic - United Arab Emirates")]
            ArabicUnitedArabEmirates,

            [System.ComponentModel.Description("Arabic - Yemen")]
            ArabicYemen,

            [System.ComponentModel.Description("Armenian - Armenia")]
            ArmenianArmenia,

            [System.ComponentModel.Description("AzeriCyrillic) - Azerbaijan")]
            AzeriCyrillicAzerbaijan,

            [System.ComponentModel.Description("Azeri (Latin) - Azerbaijan")]
            AzeriLatinAzerbaijan,

            [System.ComponentModel.Description("Basque - Basque")]
            BasqueBasque,

            [System.ComponentModel.Description("Belarusian - Belarus")]
            BelarusianBelarus,

            [System.ComponentModel.Description("Bulgarian - Bulgaria")]
            BulgarianBulgaria,

            [System.ComponentModel.Description("Catalan - Catalan")]
            CatalanCatalan,

            [System.ComponentModel.Description("Chinese - China")]
            ChineseChina,

            [System.ComponentModel.Description("Chinese - Hong Kong SAR")]
            ChineseHongKongSAR,

            [System.ComponentModel.Description("Chinese - Macau SAR")]
            ChineseMacauSAR,

            [System.ComponentModel.Description("Chinese - Singapore")]
            ChineseSingapore,

            [System.ComponentModel.Description("Chinese - Taiwan")]
            ChineseTaiwan,

            [System.ComponentModel.Description("ChineseSimplified)")]
            ChineseSimplified,

            [System.ComponentModel.Description("Chinese (Traditional)")]
            ChineseTraditional,

            [System.ComponentModel.Description("CroatianCroatia")]
            CroatianCroatia,

            [System.ComponentModel.Description("Czech - Czech Republic")]
            CzechCzechRepublic,

            [System.ComponentModel.Description("Danish - Denmark")]
            DanishDenmark,

            [System.ComponentModel.Description("Dhivehi - Maldives")]
            DhivehiMaldives,

            [System.ComponentModel.Description("Dutch - Belgium")]
            DutchBelgium,

            [System.ComponentModel.Description("Dutch - The Netherlands")]
            DutchTheNetherlands,

            [System.ComponentModel.Description("English - Australia")]
            EnglishAustralia,

            [System.ComponentModel.Description("English - Belize")]
            EnglishBelize,

            [System.ComponentModel.Description("English - Canada")]
            EnglishCanada,

            [System.ComponentModel.Description("English - Caribbean")]
            EnglishCaribbean,

            [System.ComponentModel.Description("English - Ireland")]
            EnglishIreland,

            [System.ComponentModel.Description("English - Jamaica")]
            EnglishJamaica,

            [System.ComponentModel.Description("English - New Zealand")]
            EnglishNewZealand,

            [System.ComponentModel.Description("English - Philippines")]
            EnglishPhilippines,

            [System.ComponentModel.Description("English - South Africa")]
            EnglishSouthAfrica,

            [System.ComponentModel.Description("English - Trinidad and Tobago")]
            EnglishTrinidadAndTobago,

            [System.ComponentModel.Description("English - United Kingdom")]
            EnglishUnitedKingdom,

            [System.ComponentModel.Description("English - United States")]
            EnglishUnitedStates,

            [System.ComponentModel.Description("English - Zimbabwe")]
            EnglishZimbabwe,

            [System.ComponentModel.Description("Estonian - Estonia")]
            EstonianEstonia,

            [System.ComponentModel.Description("Faroese - Faroe Islands")]
            FaroeseFaroeIslands,

            [System.ComponentModel.Description("Farsi - Iran")]
            FarsiIran,

            [System.ComponentModel.Description("Finnish - Finland")]
            FinnishFinland,

            [System.ComponentModel.Description("French - Belgium")]
            FrenchBelgium,

            [System.ComponentModel.Description("French - Canada")]
            FrenchCanada,

            [System.ComponentModel.Description("French - France")]
            FrenchFrance,

            [System.ComponentModel.Description("French - Luxembourg")]
            FrenchLuxembourg,

            [System.ComponentModel.Description("French - Monaco")]
            FrenchMonaco,

            [System.ComponentModel.Description("French - Switzerland")]
            FrenchSwitzerland,

            [System.ComponentModel.Description("Galician - Galician")]
            GalicianGalician,

            [System.ComponentModel.Description("Georgian - Georgia")]
            GeorgianGeorgia,

            [System.ComponentModel.Description("German - Austria")]
            GermanAustria,

            [System.ComponentModel.Description("German - Germany")]
            GermanGermany,

            [System.ComponentModel.Description("German - Liechtenstein")]
            GermanLiechtenstein,

            [System.ComponentModel.Description("German - Luxembourg")]
            GermanLuxembourg,

            [System.ComponentModel.Description("German - Switzerland")]
            GermanSwitzerland,

            [System.ComponentModel.Description("Greek - Greece")]
            GreekGreece,

            [System.ComponentModel.Description("Gujarati - India")]
            GujaratiIndia,

            [System.ComponentModel.Description("Hebrew - Israel")]
            HebrewIsrael,

            [System.ComponentModel.Description("Hindi - India")]
            HindiIndia,

            [System.ComponentModel.Description("Hungarian - Hungary")]
            HungarianHungary,

            [System.ComponentModel.Description("Icelandic - Iceland")]
            IcelandicIceland,

            [System.ComponentModel.Description("Indonesian - Indonesia")]
            IndonesianIndonesia,

            [System.ComponentModel.Description("Italian - Italy")]
            ItalianItaly,

            [System.ComponentModel.Description("Italian - Switzerland")]
            ItalianSwitzerland,

            [System.ComponentModel.Description("Japanese - Japan")]
            JapaneseJapan,

            [System.ComponentModel.Description("Kannada - India")]
            KannadaIndia,

            [System.ComponentModel.Description("Kazakh - Kazakhstan")]
            KazakhKazakhstan,

            [System.ComponentModel.Description("Konkani - India")]
            KonkaniIndia,

            [System.ComponentModel.Description("Korean - Korea")]
            KoreanKorea,

            [System.ComponentModel.Description("Kyrgyz - Kazakhstan")]
            KyrgyzKazakhstan,

            [System.ComponentModel.Description("Latvian - Latvia")]
            LatvianLatvia,

            [System.ComponentModel.Description("Lithuanian - Lithuania")]
            LithuanianLithuania,

            [System.ComponentModel.Description("Macedonian")]
            Macedonian,

            [System.ComponentModel.Description("MalayBrunei")]
            MalayBrunei,

            [System.ComponentModel.Description("Malay - Malaysia")]
            MalayMalaysia,

            [System.ComponentModel.Description("Marathi - India")]
            MarathiIndia,

            [System.ComponentModel.Description("Mongolian - Mongolia")]
            MongolianMongolia,

            [System.ComponentModel.Description("NorwegianBokmål) - Norway")]
            NorwegianBokmalNorway,

            [System.ComponentModel.Description("Norwegian (Nynorsk) - Norway")]
            NorwegianNynorskNorway,

            [System.ComponentModel.Description("Polish - Poland")]
            PolishPoland,

            [System.ComponentModel.Description("Portuguese - Brazil")]
            PortugueseBrazil,

            [System.ComponentModel.Description("Portuguese - Portugal")]
            PortuguesePortugal,

            [System.ComponentModel.Description("Punjabi - India")]
            PunjabiIndia,

            [System.ComponentModel.Description("Romanian - Romania")]
            RomanianRomania,

            [System.ComponentModel.Description("Russian - Russia")]
            RussianRussia,

            [System.ComponentModel.Description("Sanskrit - India")]
            SanskritIndia,

            [System.ComponentModel.Description("SerbianCyrillic) - Serbia")]
            SerbianCyrillicSerbia,

            [System.ComponentModel.Description("Serbian (Latin) - Serbia")]
            SerbianLatinSerbia,

            [System.ComponentModel.Description("Slovak - Slovakia")]
            SlovakSlovakia,

            [System.ComponentModel.Description("Slovenian - Slovenia")]
            SlovenianSlovenia,

            [System.ComponentModel.Description("Spanish - Argentina")]
            SpanishArgentina,

            [System.ComponentModel.Description("Spanish - Bolivia")]
            SpanishBolivia,

            [System.ComponentModel.Description("Spanish - Chile")]
            SpanishChile,

            [System.ComponentModel.Description("Spanish - Colombia")]
            SpanishColombia,

            [System.ComponentModel.Description("Spanish - Costa Rica")]
            SpanishCostaRica,

            [System.ComponentModel.Description("Spanish - Dominican Republic")]
            SpanishDominicanRepublic,

            [System.ComponentModel.Description("Spanish - Ecuador")]
            SpanishEcuador,

            [System.ComponentModel.Description("Spanish - El Salvador")]
            SpanishElSalvador,

            [System.ComponentModel.Description("Spanish - Guatemala")]
            SpanishGuatemala,

            [System.ComponentModel.Description("Spanish - Honduras")]
            SpanishHonduras,

            [System.ComponentModel.Description("Spanish - Mexico")]
            SpanishMexico,

            [System.ComponentModel.Description("Spanish - Nicaragua")]
            SpanishNicaragua,

            [System.ComponentModel.Description("Spanish - Panama")]
            SpanishPanama,

            [System.ComponentModel.Description("Spanish - Paraguay")]
            SpanishParaguay,

            [System.ComponentModel.Description("Spanish - Peru")]
            SpanishPeru,

            [System.ComponentModel.Description("Spanish - Puerto Rico")]
            SpanishPuertoRico,

            [System.ComponentModel.Description("Spanish - Spain")]
            SpanishSpain,

            [System.ComponentModel.Description("Spanish - Uruguay")]
            SpanishUruguay,

            [System.ComponentModel.Description("Spanish - Venezuela")]
            SpanishVenezuela,

            [System.ComponentModel.Description("Swahili - Kenya")]
            SwahiliKenya,

            [System.ComponentModel.Description("Swedish - Finland")]
            SwedishFinland,

            [System.ComponentModel.Description("Swedish - Sweden")]
            SwedishSweden,

            [System.ComponentModel.Description("Syriac - Syria")]
            SyriacSyria,

            [System.ComponentModel.Description("Tamil - India")]
            TamilIndia,

            [System.ComponentModel.Description("Tatar - Russia")]
            TatarRussia,

            [System.ComponentModel.Description("Telugu - India")]
            TeluguIndia,

            [System.ComponentModel.Description("Thai - Thailand")]
            ThaiThailand,

            [System.ComponentModel.Description("Turkish - Turkey")]
            TurkishTurkey,

            [System.ComponentModel.Description("Ukrainian - Ukraine")]
            UkrainianUkraine,

            [System.ComponentModel.Description("Urdu - Pakistan")]
            UrduPakistan,

            [System.ComponentModel.Description("UzbekCyrillic) - Uzbekistan")]
            UzbekCyrillicUzbekistan,

            [System.ComponentModel.Description("Uzbek (Latin) - Uzbekistan")]
            UzbekLatinUzbekistan,

            [System.ComponentModel.Description("Vietnamese - Vietnam")]
            VietnameseVietnam,
        }

        private class CultureData
        {
            public CultureData()
            {
            }

            public string Id { get; set; }
            public string Identifier { get; set; }
            public string CultureName { get; set; }
            public string Title { get; set; }
            //public CS.General_v4.Enums.CountryIso3166 CultureCountryCode { get; set; }
            //public CS.General_v4.Enums.CurrencyCode_Iso4217 DefaultCurrency { get; set; }

            public LanguageCultureName? LanguageCultureName { get; set; }
            //public CS.General_v4.Enums.LanguageCode_Iso639 Language { get; set; }
            public bool IsDefaultCulture { get; set; }


        }

        [Fact]
        public void SortByNotWorkingTest()
        {
            using (var store = GetDocumentStore())
            {
                new CultureDataIndex().Execute(store);
            }
        }
    }
}
