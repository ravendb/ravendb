

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

