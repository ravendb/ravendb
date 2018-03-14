

SET IDENTITY_INSERT [order] ON
INSERT INTO [order] ([o_id],[total]) VALUES(1, 440.00)
SET IDENTITY_INSERT [order] OFF


SET IDENTITY_INSERT [product] ON
INSERT INTO [product] ([p_id],[name]) VALUES(100, 'Bread')
INSERT INTO [product] ([p_id],[name]) VALUES(101, 'Milk')
SET IDENTITY_INSERT [product] OFF

SET IDENTITY_INSERT [order_item] ON
INSERT INTO [order_item] ([oi_id],[order_id],[product_id],[price]) VALUES(10,1,100,110.00)
INSERT INTO [order_item] ([oi_id],[order_id],[product_id],[price]) VALUES(11,1,101,330.0)
SET IDENTITY_INSERT [order_item] OFF


