import asyncio
import aiohttp
import time


async def hacer_pregunta(session, id_pregunta):
    print(f"-> Mandando pregunta {id_pregunta}...")
    url = "http://localhost:8000/search?query=¿Cuál%20es%20el%20suministro%20máximo%20o%20límite%20absoluto%20de%20bitcoins%20que%20van%20a%20existir%20en%20la%20historia%20y%20cada%20cuánto%20ocurre%20el%20evento%20llamado%20halving?"
    async with session.get(url) as response:
        res = await response.json()
        print(f"<- Respuesta {id_pregunta} recibida!")
        return res


async def main():
    inicio = time.time()

    # Abrimos una sesión para mandar múltiples peticiones a la vez
    async with aiohttp.ClientSession() as session:
        # Armamos una lista de 5 peticiones simultáneas
        tareas = [hacer_pregunta(session, i) for i in range(1, 6)]

        # Ejecutamos las 5 EXACTAMENTE al mismo tiempo
        await asyncio.gather(*tareas)

    fin = time.time()
    print(f"\n[!] Tiempo total para 5 peticiones: {fin - inicio:.2f} segundos")


if __name__ == "__main__":
    asyncio.run(main())
