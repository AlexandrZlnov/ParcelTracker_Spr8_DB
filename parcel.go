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
	res, err := s.db.Exec("INSERT INTO parcel (client, status, address, created_at) VALUES (:client, :status, :address, :created_at)",
		sql.Named("client", p.Client),
		sql.Named("status", p.Status),
		sql.Named("address", p.Address),
		sql.Named("created_at", p.CreatedAt),
	)
	if err != nil {
		return 0, fmt.Errorf("error adding line: %w", err)
	}

	// возвращаем идентификатор последней добавленной записи
	lastId, err := res.LastInsertId()
	if err != nil {
		return 0, fmt.Errorf("error getting last record: %w", err)
	}
	return int(lastId), nil
}

func (s ParcelStore) Get(number int) (Parcel, error) {
	row := s.db.QueryRow("SELECT number, client, status, address, created_at FROM parcel WHERE number = ?", number)

	// заполняем объект Parcel данными из таблицы
	p := Parcel{}
	err := row.Scan(&p.Number, &p.Client, &p.Status, &p.Address, &p.CreatedAt)
	if err != nil {
		return p, fmt.Errorf("error filling Parcel structure with data from table %w", err)
	}
	return p, nil
}

func (s ParcelStore) GetByClient(client int) ([]Parcel, error) {
	rows, err := s.db.Query("SELECT number, client, status, address, created_at FROM parcel WHERE client = :client",
		sql.Named("client", client))
	if err != nil {
		return nil, fmt.Errorf("error reading table: %w", err)
	}
	defer rows.Close()

	// заполнен срез Parcel данными из таблицы
	var res []Parcel
	for rows.Next() {
		parcel := Parcel{}
		err = rows.Scan(&parcel.Number, &parcel.Client, &parcel.Status, &parcel.Address, &parcel.CreatedAt)
		if err != nil {
			return res, fmt.Errorf("error filling Parcel structure with data from table %w", err)
		}
		res = append(res, parcel)
	}
	if err = rows.Err(); err != nil {
		return res, fmt.Errorf("error from iterating rows: %w", err)
	}
	return res, nil
}

func (s ParcelStore) SetStatus(number int, status string) error {
	// обновление статуса в таблице parcel
	_, err := s.db.Exec("UPDATE parcel SET status = :status WHERE number = :number",
		sql.Named("status", status),
		sql.Named("number", number))

	if err != nil {
		return fmt.Errorf("error updating status: %w", err)
	}
	return nil
}

func (s ParcelStore) SetAddress(number int, address string) error {
	// обновление адреса в таблице parcel
	// адрес можно менять только если значение статуса registered
	_, err := s.db.Exec("UPDATE parcel SET address = :address WHERE number = :number and status = :registered",
		sql.Named("address", address),
		sql.Named("number", number),
		sql.Named("registered", ParcelStatusRegistered))
	if err != nil {
		return fmt.Errorf("error updating address: %w", err)
	}
	return nil
}

func (s ParcelStore) Delete(number int) error {
	// удаление строки из таблицы parcel
	// удалять строку можно только если значение статуса registered

	_, err := s.db.Exec("DELETE FROM parcel WHERE number = :number and status = :status",
		sql.Named("number", number),
		sql.Named("status", ParcelStatusRegistered))
	if err != nil {
		return fmt.Errorf("error deleting line: %w", err)
	}
	return nil
}
