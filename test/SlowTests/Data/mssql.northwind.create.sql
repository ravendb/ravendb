

/*==============================================================*/
/* Table: NoPkTable                                              */
/*==============================================================*/
create table NoPkTable (
   Id                   int                  identity,  
)



/*==============================================================*/
/* Table: UnsupportedTable                                      */
/*==============================================================*/
create table UnsupportedTable (
   Id                   int                  identity, 
   Node                 hierarchyid          not null,
   constraint PK_UnsupportedTable primary key (Id) 
)


/*==============================================================*/
/* Table: Customer                                              */
/*==============================================================*/
create table Customer (
   Id                   int                  identity,
   FirstName            nvarchar(40)         not null,
   Pic			        varbinary(MAX)	     null,
   constraint PK_CUSTOMER primary key (Id)
)


/*==============================================================*/
/* Table: Order                                               */
/*==============================================================*/
create table "Order" (
   Id                   int                  identity,
   OrderDate            datetime             not null default getdate(),
   CustomerId           int                  not null,
   TotalAmount          decimal(12,2)        null default 0,
   constraint PK_ORDER primary key (Id)
)


/*==============================================================*/
/* Table: OrderItem                                             */
/*==============================================================*/
create table OrderItem (  
   OrderId              int                  identity,
   ProductId            int                  not null,
   UnitPrice            decimal(12,2)        not null default 0,
   constraint PK_ORDERITEM primary key (OrderID, ProductID)
)



/*==============================================================*/
/* Table: Details                                               */
/*==============================================================*/
create table Details (  
   ID                   int                  identity,
   OrderId              int                  not null,
   ProductId            int                  not null,
   Name			        nvarchar(30)	     null,
   constraint PK_DETAILS primary key (ID, OrderID, ProductID)
)


/*==============================================================*/
/* Table: Product                                               */
/*==============================================================*/
create table Product (
   Id                   int                  identity,
   UnitPrice            decimal(12,2)        null default 0,
   IsDiscontinued       bit                  not null default 0,
   constraint PK_PRODUCT primary key (Id)
)

/*==============================================================*/
/* Table: Category                                              */
/*==============================================================*/
create table Category (
   Id                   int                  identity,
   Name			        nvarchar(30)	     not null,
   constraint PK_CATEGORY primary key (Id)
)


/*==============================================================*/
/* Table: ProductCategory                                       */
/*==============================================================*/
create table ProductCategory (
   ProductId              int                  not null,
   CategoryId             int                  not null,
   constraint PK_PRODUCT_CATEGORY primary key (ProductId, CategoryId)
)


/*==============================================================*/
/* Table: Photo                                              */
/*==============================================================*/
create table Photo (  
   Id			int		             identity,
   Pic			varbinary(MAX)	     null,	   
   Photographer	int		             null,
   InPic1		int                  null,
   InPic2		int		             null,
   constraint PK_Photo primary key (Id)
)


alter table "Order"
   add constraint FK_ORDER_REFERENCE_CUSTOMER foreign key (CustomerId)
      references Customer (Id)


alter table OrderItem
   add constraint FK_ORDERITE_REFERENCE_ORDER foreign key (OrderId)
      references "Order" (Id)


alter table Details
   add constraint FK_Details_REFERENCE_ORDERITEM foreign key (OrderId, ProductId)
      references OrderItem(OrderId, ProductId)

alter table OrderItem
   add constraint FK_ORDERITE_REFERENCE_PRODUCT foreign key (ProductId)
      references Product (Id)


alter table Photo
   add constraint FK_ORDERITE_REFERENCE_CUSTOMER1 foreign key (Photographer)
      references Customer (Id)


alter table Photo
   add constraint FK_ORDERITE_REFERENCE_CUSTOMER2 foreign key (InPic1)
      references Customer (Id)


alter table Photo
   add constraint FK_ORDERITE_REFERENCE_CUSTOMER3 foreign key (InPic2)
      references Customer (Id)

ALTER TABLE ProductCategory
    ADD CONSTRAINT FK_PROD_CAT_REF_PROD FOREIGN KEY (ProductId)
      REFERENCES Product (Id)
      
ALTER TABLE ProductCategory
    ADD CONSTRAINT FK_PROD_CAT_REF_CAT FOREIGN KEY (CategoryId)
        REFERENCES Category(Id)

 GO 

CREATE VIEW john_customers
AS SELECT * FROM Customer
WHERE FirstName = 'John';
