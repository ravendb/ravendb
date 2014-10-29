using Raven.Imports.Newtonsoft.Json;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
	public class Raven_json_serialization_fail_tests : RavenTest
	{
		[JsonObject(IsReference = true)]
		public class Contract
		{
			public Contract()
			{
				ContractDetail = new ContractDetail();
				CurrencyContract = new CurrencyContract();
				Calculator = new Calculator(this);
			}

			public string Id { get; set; }
			public ContractDetail ContractDetail { get; set; }
			public CurrencyContract CurrencyContract { get; set; }
			public Calculator Calculator { get; set; }

			public void Init()
			{
				Calculator.Init(this);
			}
		}

		public class ContractDetail
		{
			public decimal Quantity { get; set; }
			public decimal Price { get; set; }
		}

		public class CurrencyContract
		{
			public string Number { get; set; }
			public decimal ExchangeRate { get; set; }
		}


		public class Calculator
		{
			public Contract Contract { get; set; }

			public Calculator(Contract contract)
			{
				Contract = contract;
			}

			public decimal ContractValueUsd
			{
				get { return Contract.ContractDetail.Price * Contract.ContractDetail.Quantity; }
			}

			public decimal ContractValueReais
			{
				get { return Contract.ContractDetail.Price * Contract.ContractDetail.Quantity * Contract.CurrencyContract.ExchangeRate; }
			}

			public void Init(Contract contract)
			{
				Contract = contract;
			}
		}

		private static Contract GetContract()
		{
			var contract = new Contract
			{
				Id = "id/1",
				ContractDetail = new ContractDetail
				{
					Price = 50,
					Quantity = 3
				},
				CurrencyContract = new CurrencyContract { Number = "abc", ExchangeRate = 2.03M }
			};
			return contract;
		}

		// Passing test. The reference to the parent Contract in Contract.Calculator is manually called 
		// AFTER the database loads the contract
		[Fact]
		public void Calculator_is_manually_initialized_after_loading_from_db()
		{
			// Arrange
			var contract = GetContract();

			// Act

			using (var store = NewDocumentStore())
			{
				Contract result;
				using (var writeSession = store.OpenSession())
				{
					writeSession.Store(contract);
					writeSession.SaveChanges();
				}

				using (var readSession = store.OpenSession())
				{
					result = readSession.Load<Contract>("id/1");
					result.Init();
				}

				// Assert
				Assert.Equal(result.Calculator.ContractValueUsd, 150.0M);
				Assert.Equal(result.Calculator.ContractValueReais, 304.50M);
			}
		}

		// Passing test. Simply illustrates that Calculator is correctly initialized in Contract.ctor, and all 
		// the properties of Contract are available to Calculator
		[Fact]
		public void Calculator_should_be_initialized_in_contracts_ctor()
		{
			// Arrange
			var result = GetContract();

			// Assert
			Assert.Equal(result.Calculator.ContractValueUsd, 150.0M);
			Assert.Equal(result.Calculator.ContractValueReais, 304.50M);
			
		}

		// Failing test. Having marked the Contract class as  [JsonObject(IsReference = true)], we would expect Calculator
		// to be correctly initialized after being loaded from the db.
		[Fact]
		public void Calculator_should_be_initialized_in_contracts_ctor_when_loaded_from_db_and_contract_is_marked_with_json_object_is_reference_true()
		{
			// Arrange
			var contract = GetContract();


			using (var store = NewDocumentStore())
			{
				using (var writeSession = store.OpenSession())
				{
					writeSession.Store(contract);
					writeSession.SaveChanges();
				}

				Contract result;
				using (var readSession = store.OpenSession())
				{
					result = readSession.Load<Contract>("id/1");
				}

				WaitForUserToContinueTheTest(store);

				Assert.Equal(result.Calculator.ContractValueUsd, 150.0M);
				Assert.Equal(result.Calculator.ContractValueReais, 304.50M);
	
			}
		}
	}
}