package core

import (
	"ConcarenncyHits/internal/service"
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sync"
	"syscall"
)

type DataFromUser struct {
	groupsCount     int
	devicesPerGroup int
	storageCapacity int
}

func TakeDataFromUser() DataFromUser {
	var data DataFromUser

	for {
		fmt.Print("Введите ёмкость центрального накопителя: ")
		_, err := fmt.Fscan(os.Stdin, &data.storageCapacity)
		if err == nil && data.storageCapacity > 0 {
			break
		}
		fmt.Println("Ёмкость должна быть больше 0")
		clearInputLine()
	}

	for {
		fmt.Print("Введите количество групп оборудования n, где n > 2: ")
		_, err := fmt.Fscan(os.Stdin, &data.groupsCount)
		if err == nil && data.groupsCount > 2 {
			break
		}
		fmt.Println("Количество групп должно быть больше 2")
		clearInputLine()
	}

	for {
		fmt.Print("Введите количество устройств в каждой группе m, где m > 3: ")
		_, err := fmt.Fscan(os.Stdin, &data.devicesPerGroup)
		if err == nil && data.devicesPerGroup > 3 {
			break
		}
		fmt.Println("Количество устройств должно быть больше 3")
		clearInputLine()
	}

	clearInputLine()
	return data
}

func StartWorking() {
	data := TakeDataFromUser()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := service.Config{
		StorageCapacity: data.storageCapacity,
		GroupsCount:     data.groupsCount,
		DevicesPerGroup: data.devicesPerGroup,
	}

	system := service.NewSystem(cfg)

	var wg sync.WaitGroup
	system.Run(ctx, &wg)

	fmt.Println("Система запущена. Нажмите Enter или Ctrl+C для корректной остановки.")

	signalCh := make(chan os.Signal, 1)
	signal.Notify(signalCh, os.Interrupt, syscall.SIGTERM)

	enterCh := make(chan struct{})
	go func() {
		_, _ = bufio.NewReader(os.Stdin).ReadString('\n')
		close(enterCh)
	}()

	select {
	case <-signalCh:
		fmt.Println("Получен сигнал завершения")
	case <-enterCh:
		fmt.Println("Получена команда завершения")
	}

	cancel()
	wg.Wait()
	fmt.Println("Все потоки корректно остановлены")
}

func clearInputLine() {
	reader := bufio.NewReader(os.Stdin)
	_, _ = reader.ReadString('\n')
}
