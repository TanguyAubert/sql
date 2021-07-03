#include <iostream>
#include "code/tests.h"
#include "code/examples-orc-decla.h"
#include "code/examples-envoi-bce.h"
#include "code/examples-secure-desktop.h"

auto main() -> int
{
    SQL::run_all_tests();
    SQL::example_2();
    SQL::example_3();
    SQL::example_4();
    
    return 0;
}

// 30746 / 31471 / 31475 / 31644 / 30134 / 30130 / 30130
// 28058 / 28744 / 28748 / 28589 / 27058 / 27054 / 27054
// 15793 / 16101 / 16119 / 16214 / 14983 / 14979 / 24471

// TO DO
// Voir pourquoi la sélection avec * lors des Standard et Merger ne fonctionnent pas correctement
// Trouver comment réduire le temps d'exécution

// MAYBE
// Et créer une fonction reorder pour mettre certaines colonnes en premier => PAS NECESSAIRE
// Créer des fonctions du type over_partition_by pour simplifier l'écriture => NON et supprimer les fonctions sum, count et over_partition_by
// Tenir compte du test N°2 pour simplifier encore la requête
// Vérifier au moment du parse que les parenthèses sont bien équilibrées => DIFFICILE

// NOT TO DO
// Dans la fonction create_column, remonter dans les predécesseurs pour trouver le bon emplacement => NON pour pouvoir utiliser le select * et supprimer la variable successor
// Reprendre la fonction add de la classe Merger pour qu'elle ne crée pas d'intermédiaire lorsque le prédecesseur est de classe Standard et qu'aucune des colonnes du by sont des colonnes créées (mettre les filtres dans le ON s'il y en a) ! Faire attention au partition by s'il y en a ??? => problématique s'il y a un alias.* !!!
// Dans la fonction aggregate, vérifier que les instructions n'utilisent pas de colonnes calculées ?
// Réordonner les colonnes au moment de la sélection ? => NON car cela empêcherait l'utilisation de alias.*
// Dans la classe Merger, donner le type Node à lhs et rhs au lieu de Standard ??? => NON
// Créer une API en R ou appeler le programme C++ en ligne de commande ???
// Ajouter une table Standard par défaut juste après la création de la table initiale ??? => NON

// DONE
// Supprimer la variable successor dans la classe Node !
// Supprimer les jointures inutiles
// Supprimer une jointure s'il s'agit d'un left join, que la table de droite ne contient plus aucune colonne active en dehors des colonnes utilisées pour la jointure et que les colonnes utilisées pour la jointure garantissent l'unicité de chaque ligne
// Simplifier la chaîne des tables (suppression des tables Standard et des jointures qui n'apportent rien) au moment du render de la classe Table
// Utiliser alias.* si toutes les colonnes de l'étape précédente existent telles qu'elles dans l'étape courante
// Utiliser la fonction ABS dans le calcul de bsi_instrument_balance pour réduire encore la taille de la requête
// Vérifier dans la fonction rename que le nouveau nom n'existe pas déjà !
// Utilier * ou la liste des colonnes lors d'une jointure ? La même chose est-elle possible sur la table de droite ???
// Regrouper les fonctions aggregate et by pour réduire la quantité de code et simplifier les vérifications !!!
// Supprimer des espaces autour des virgules !
// Programmer une fonction "unique" utilisant le mot-clé DISTINCT
// Créer une fonction pour tester si une colonne existe dans la table
// Vérifier que tout fonctionne sur les tests !
// Programmer la fonction Stack et ajouter une classe Stacker !
// Découper le programme en plusieurs fichiers .h
// Ne pas désactiver les colonnes utilisées dans le GROUP BY lors d'une agrégation ??? => ajouter une table Standard si c'est le cas
// De même, ajouter une table Standard si l'utilisateur tente de désactiver des colonnes utilisées comme clés de jointure
// Dans la classe Aggregate, vérifier qu'il n'y a pas de colonnes en doublons (parmi les colonnes créées et le group by)
// Stocker les erreurs pour pouvoir les réutiliser plus tard !
// Remplacer les == par des = ??? OU afficher un message d'erreur ???
// Gérer les échappements de caractères du genre \' ou \" à l'intérieur d'une chaîne de caractères dans la fonction parse
// Créer une fonction drop_key prenant en premier paramètre la clé à supprimer et en deuxième paramètre l'unique valeur à conserver
// Supprimer les espaces entre les noms de colonnes
// Si la jointure est un LEFT JOIN et que l'ensemble des clés de la table de droite se trouvent dans le BY, ne pas ajouter ces clés à la liste des clés de la nouvelle table. De même pour un RIGHT JOIN 
// Lors d'une jointure, faire keys = {} si lhs->get_keys().empty() || rhs->get_keys().empty(). De même lors d'un empilement
// Propager les clés d'une table à l'autre mais en gardant la possibilité pour l'utilisateur de respécifier les clés à tout moment
// Afficher une erreur lorsque la jointure n'utilise pas toutes les clés d'au moins une des tables
// Vérifier que drop_key est appliquée sur une clé et que drop est appliquée sur une colonne ne servant pas de clé !
// Supprimer les clés si l'une d'elle est désactivée
// Vérifier que les noms ne sont pas vides au moment d'un rename
// Créer une class Unique pour gérer correctement l'utilisation de DISTINCT et faire hériter cette class de Standard => NON, ajouter une étape Standard pour figer la table !!
// Modifier les clés lors d'un distinct
// Limiter le nombres de copies (en particulier, ne copier que le rhs lors d'une fusion ou d'un empilement)
// Propager le nom des tables (y compris lors d'une jointure sous la forme orc_decla_t2 / orc_decla_t3)
// Gérer les spécificités d'Oracle et de Hive !
// Simplifier les jointures automatiquement en utilisant les clés
// Supprimer les fonctions left_join_s et right_join_s
// Rajouter une fonction prefix en plus de prefix_all_except
// Réordonner les calculs des colonnes pour minimiser la longueur de la requête
// Gérer les valeurs du type 1e-6 au moment du "parse" !
// Rendre les messages d'erreur plus clairs
// Tout mettre dans les fichiers .h pour améliorer le temps de compilation ???
// Lors d'un rename sur une agrégation, renommer aussi la colonne dans les filters où elle est utilisée ???
// Tout nettoyer et simplifier :)


