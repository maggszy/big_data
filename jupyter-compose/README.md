# Jupyterowy docker z volumenem

## Co to jest i co daje

Dzięki temu plikowi można łatwo uruchomić jupytera ze sparkiem w kontenerze. Jednocześnie, nie tracimy zasobów jeśli kontener nam umrze.

## Struktura plików

Aby użyć tego rozwiązania, należy mieć osobny folder do przechowywania projektu. Nazwiemy go np. `big-data-proj` (nazwa bez znaczenia). Umieścić w nim trzeba plik `docker-compose.yaml`, ktróry tu widzicie. Innymi słowy można pobrać sobie ten folder z repo i działać w nim jak w templatce.

Folder `project` (czyli u nas `.../big-data-proj/project`) jest współdzielony między kontenerem a waszym kompem. Jeśli go nie utworzyliście, powinien utworzyć się sam przy pierwszym starcie kontenera i już tam zostać.

## Uruchomienie

### Standardowa procedura

1. Przejdź do głównego folderu projektu

    ```sh
    cd jakas/sciezka/big-data-proj
    ```

2. (*opcjonalnie*) Możesz w tym folderze utworzyć folder `project` który będzie współdzielony i przerzucić tam pliki projektu. Jeżeli jeszcze nie masz plików, folder utworzy się sam przy starcie kontenera, a to co zrobisz w jupyterze, po prostu będzie tam wgrywane.

3. Upewnij się, że silnik dockerowy działa. Jeśli to Docker Desktop, trzeba ją włączyć ręcznie.

4. Będąc cały czas na poziomie `jakas/sciezka/big-data-proj` uruchom

    ```sh
    docker compose up
    ```

5. Wejdź na loclhost:8888 przez link z ` http://127.0.0.1:8888/lab?token=..............`, który pokaże się w konsoli po pewnym czasie.

### Pierwsze uruchomienie

- Za pierwszym razrem **krok 4** będzie trwał dużej bo kontener musi się zbudować.
- Jeśli komenda nie chce działać, można spróbować też

    ```sh
    docker compose up --build
    ```

    ale tylko za pierwszym razem. Poźniej flaga `--build` jest zbędna.

### Wyłączanie kontenerów

Po wejściu w panek 'Containers' w panelu Docker Desktop można zatrzymać znaczkiem stopu to co jest na zielono. Kosza nie klikamy.

Równoważnie, w folderze z plikiem compose wpisać można

```sh
docker compose down
```
