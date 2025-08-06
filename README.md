# Steam Data Analysis

Ovaj projekat koristi **Kafka producer i consumer** kako bi se u realnom vremenu obradio dataset iz stream-a koji se nalazi u Docker kontejneru. Projekat je razvijen za **Grupu 3**, a podaci se preuzimaju sa `http://localhost:5001/stream/type3`.

Cilj projekta je da se podaci iz stream-a transformišu i sačuvaju u CSV fajlu, pri čemu se obrada prekida kada se pošalje poruka `"close"` sa strane producer-a.

---

## ▶️ Pokretanje stream-a

1. **Preuzmi Docker `.tar` fajl za grupu 3**  
   *(link ubaciti ovde kada bude dostupan)*

2. **Učitaj Docker sliku:**
```bash
docker load -i C:\Users\user\Downloads\bigdata-g3.tar
```

3. **Pokreni Docker kontejner:**
```bash
docker run -d -p 5001:5000 --name bigdata-g3 bigdata-g3:latest
```

4. **Proveri da li kontejner radi:**
Otvori u browseru:
```
http://localhost:5001/health-check
```
Ako je sve u redu, videćeš odgovor: `Success`

5. **Stream podataka:**
```
http://localhost:5001/stream/type3
```

---

## ⚙️ Pokretanje Java aplikacije

Nakon što je stream aktivan, pokreni sledeće klase:

- `Producer.java` – preuzima podatke iz stream-a i šalje ih kao Kafka poruke
- `ConsumerF.java` – Kafka consumer koji snima podatke u CSV fajl
- `ConsumerT.java` – Kafka consumer koji samo prikazuje podatke u konzoli

**Napomena:** Kada producer pošalje `"close"` poruku, consumer automatski prekida sa radom.

---

## 📂 Objašnjenje koda

### `Producer.java`
- Učitava JSON podatke sa URL-a `http://localhost:5001/stream/type3`
- Parsira svaki JSON objekat liniju po liniju
- Šalje svaki objekat kao Kafka poruku na temu `"steam-data"`
- Kada se završi stream, šalje `"close"` poruku

### `ConsumerF.java`
- Prima Kafka poruke sa teme `"steam-data"`
- Čuva sve poruke u CSV fajl (`output.csv`)
- Kada primi `"close"`, završava pisanje i zatvara fajl

### `ConsumerT.java`
- Prima Kafka poruke sa teme `"steam-data"`
- Ispisuje podatke u konzolu bez snimanja
- Takođe prekida na `"close"` poruku

---
