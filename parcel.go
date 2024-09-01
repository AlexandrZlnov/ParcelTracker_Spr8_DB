package main

import (
	"database/sql"
	"fmt"
)

type ParcelStore struct {
	db *sql.DB
}

func NewParcelStore(db *sql.DB) ParcelStore {
	return ParcelStore{db: db}
}

func (s ParcelStore) Add(p Parcel) (int, error) {
	// реализуйте добавление строки в таблицу parcel, используйте данные из переменной p
	res, err := s.db.Exec("insert into parcel (client, status, address, created_at) values (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		fmt.Println(err)
		return 0, nil
	}

	// верните идентификатор последней добавленной записи
	lastId, _ := res.LastInsertId()
	return int(lastId), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	// реализуйте чтение строки по заданному number
	// здесь из таблицы должна вернуться только одна строка
	row := s.db.QueryRow("select *from parcel where number = ?", number)

	// заполните объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		fmt.Println(err)
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	// реализуйте чтение строк из таблицы parcel по заданному client
	// здесь из таблицы может вернуться несколько строк
	rows, err := s.db.Query("select *from parcel where client = :client",
		sql.Named("client", client))
	if err != nil {
		fmt.Println(err)
	}
	defer rows.Close()

	// заполните срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		parcel := Parcel{}
		err = rows.Scan(&parcel.Number, &parcel.Client, &parcel.Status, &parcel.Address, &parcel.CreatedAt)
		if err != nil {
			fmt.Println(err)
		}
		res = append(res, parcel)
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// реализуйте обновление статуса в таблице parcel
	_, err := s.db.Exec("update parcel set status = :status where number = :number",
		sql.Named("status", status),
		sql.Named("number", number))

	if err != nil {
		fmt.Println(err)
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// реализуйте обновление адреса в таблице parcel
	// менять адрес можно только если значение статуса registered
	_, err := s.db.Exec("update parcel set address = :address where number = :number and status = :registered",
		sql.Named("address", address),
		sql.Named("number", number),
		sql.Named("registered", ParcelStatusRegistered))
	if err != nil {
		fmt.Println(err)
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	// реализуйте удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered

	_, err := s.db.Exec("delete from parcel where number = :number and status = :status",
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered))
	if err != nil {
		fmt.Println(err)
	}

	return nil
}
