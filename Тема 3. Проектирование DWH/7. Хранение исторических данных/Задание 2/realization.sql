
-- Удалите внешний ключ из sales
alter table de.public.sales drop constraint sales_products_product_id_fk;

-- Удалите первичный ключ из products
alter table de.public.products drop constraint products_pk;

-- Добавьте новое поле id для сурогатного ключа в products
alter table de.public.products add id serial;

-- Сделайте данное поле первичным ключом
alter table de.public.products add constraint products_pk primary key (id);

-- Добавьте дату начала действия записи в products; Добавьте дату окончания действия записи в products
alter table de.public.products add valid_from timestamptz,add valid_to timestamptz;

-- Добавьте новый внешний ключ sales_products_id_fk в sales
alter table de.public.sales add constraint sales_products_id_fk foreign key (product_id) references de.public.products;