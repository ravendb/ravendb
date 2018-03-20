

create table "order" (
   o_id           int                  identity,
   total          decimal(12,2)        null default 0,
   constraint PK_ORDER primary key (o_id)
)

create table order_item (  
   oi_id                int                  IDENTITY,
   order_id             int                  NULL,
   product_id           INT                  NOT NULL,
   price            decimal(12,2)        not null default 0,
   constraint PK_ORDERITEM primary key (oi_id)
)

create table product (
    p_id       INT                     IDENTITY,
    name       nvarchar(200)           NOT NULL,
    CONSTRAINT PK_PRODUCT PRIMARY KEY (p_id)
)

alter table order_item
   add constraint FK_ORDERITE_REFERENCE_ORDER foreign key (order_id)
      references "order" (o_id)


ALTER TABLE order_item 
    ADD CONSTRAINT FK_PRODUCT_ITEM FOREIGN KEY (product_id)
        REFERENCES "product" (p_id)
