# mr_track_count.py

from mrjob.job import MRJob

# Ce job calcule le nombre total de tracks par album à partir du fichier tracks.csv.
class MRAlbumTrackCount(MRJob):

    # 1. MAPPER : Lit chaque ligne du CSV et émet l'album_id comme clé.
    def mapper(self, _, line):
        """
        Le protocole par défaut (RawValueProtocol) est utilisé ici, 
        il lit chaque ligne du fichier comme une chaîne de caractères ('line').
        """
        try:
            # Séparer les colonnes (suppose un délimiteur virgule)
            fields = line.strip().split(',')
            
            # Définir l'index de la colonne pour l'ID de l'album.
            # ******************************************************
            # ASSUMPTION: L'ID de l'album (album_id) est la première colonne (Index 0).
            # VOUS DEVEZ AJUSTER CET INDEX si la colonne est différente dans votre fichier tracks.csv.
            # ******************************************************
            ALBUM_ID_INDEX = 0
            
            album_id = fields[ALBUM_ID_INDEX].strip()

            # Filtrer la ligne d'en-tête (souvent le cas dans les CSV) et les lignes non numériques.
            # On vérifie si la valeur de l'ID est un nombre (ou une chaîne d'ID valide)
            if album_id.isdigit():
                # Clé: album_id, Valeur: 1 (pour un décompte)
                yield album_id, 1
            
        except Exception:
            # Ignorer les lignes corrompues ou l'en-tête du fichier
            pass

    # 2. REDUCER : Reçoit (album_id, [1, 1, 1, ...]) et retourne le total.
    def reducer(self, album_id, counts):
        """
        Reçoit l'ID de l'album et un itérateur de '1' pour chaque track.
        Somme toutes les valeurs pour obtenir le décompte total.
        """
        # Résultat: (album_id, nombre_total_de_tracks_sur_cet_album)
        yield album_id, sum(counts)

if __name__ == '__main__':
    MRAlbumTrackCount.run()