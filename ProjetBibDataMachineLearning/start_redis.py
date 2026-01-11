# Script pour vérifier et démarrer Redis automatiquement
import subprocess
import time
import sys
import os
import redis

def check_redis_running():
    """Vérifie si Redis est en cours d'exécution"""
    try:
        r = redis.Redis(host='localhost', port=6379)
        r.ping()
        return True
    except:
        return False

def start_redis():
    """Démarre Redis"""
    print("Démarrage de Redis...")
    try:
        # Essayer de démarrer Redis
        subprocess.Popen(['redis-server.exe'], 
                        stdout=subprocess.DEVNULL,
                        stderr=subprocess.DEVNULL)
        
        # Attendre que Redis démarre
        time.sleep(2)
        
        # Vérifier que c'est bien démarré
        if check_redis_running():
            print("✓ Redis démarré avec succès!")
            return True
        else:
            print("✗ Redis n'a pas démarré correctement")
            return False
    except Exception as e:
        print(f"✗ Impossible de démarrer Redis: {e}")
        print("\nAssurez-vous que Redis est installé dans le PATH")
        return False

def main():
    print("=" * 60)
    print("VÉRIFICATION ET DÉMARRAGE DE REDIS")
    print("=" * 60)
    
    if check_redis_running():
        print("\n✓ Redis est déjà en cours d'exécution!")
        print("  Vous pouvez maintenant lancer: python run_realtime.py")
    else:
        print("\nRedis n'est pas en cours d'exécution")
        if start_redis():
            print("\nVous pouvez maintenant lancer: python run_realtime.py")
        else:
            print("\nVeuillez démarrer Redis manuellement:")
            print("  Ouvrez PowerShell et exécutez: redis-server.exe")

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\n\nArrêt")
