

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


SET IDENTITY_INSERT [movie] ON 
INSERT INTO [movie] ([m_id], [name], [file]) VALUES (21, 'Movie #1', CONVERT(VARBINARY, '21'))
INSERT INTO [movie] ([m_id], [name], [file]) VALUES (22, 'Movie #2', null)
INSERT INTO [movie] ([m_id], [name], [file]) VALUES (23, 'Movie #3', CONVERT(VARBINARY, '23'))
INSERT INTO [movie] ([m_id], [name], [file]) VALUES (24, 'Movie #4', null)
INSERT INTO [movie] ([m_id], [name], [file]) VALUES (25, 'Movie #5', null)
SET IDENTITY_INSERT [movie] OFF


SET IDENTITY_INSERT [actor] ON
INSERT INTO [actor] ([a_id], [name], [photo]) VALUES (31, 'Actor #1', CONVERT(VARBINARY, '31'))
INSERT INTO [actor] ([a_id], [name], [photo]) VALUES (32, 'Actor #2', CONVERT(VARBINARY, '32'))
INSERT INTO [actor] ([a_id], [name], [photo]) VALUES (33, 'Actor #3', null)
INSERT INTO [actor] ([a_id], [name], [photo]) VALUES (34, 'Actor #4', null)
SET IDENTITY_INSERT [actor] OFF 

INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (21, 31)
INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (21, 32)

INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (22, 31)
INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (22, 33)

INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (23, 32)

INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (24, 32)
INSERT INTO [actor_movie] ([m_id], [a_id]) VALUES (24, 33)


SET IDENTITY_INSERT [groups1] ON
INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (50, 'G1', null);

INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (51, 'G1.1', 50);
INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (52, 'G1.1.1', 51);
INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (53, 'G1.1.1.1', 52);

INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (54, 'G1.1.2', 51);

INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (55, 'G2', null);

INSERT INTO [groups1] ([g_id], [name], [parent_group_id]) VALUES (56, 'G2.1', 55);
SET IDENTITY_INSERT [groups1] OFF


SET IDENTITY_INSERT [customers2] ON
INSERT INTO [customers2] ([c_id], [vatid]) VALUES (61, 55555);
INSERT INTO [customers2] ([c_id], [vatid]) VALUES (62, 44444);
SET IDENTITY_INSERT [customers2] OFF

SET IDENTITY_INSERT [orders2] ON
INSERT INTO [orders2] ([o_id], [customer_vatid]) VALUES (71, 55555);
INSERT INTO [orders2] ([o_id], [customer_vatid]) VALUES (72, 55555);
INSERT INTO [orders2] ([o_id], [customer_vatid]) VALUES (73, 44444);
SET IDENTITY_INSERT [orders2] OFF
