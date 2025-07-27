// backend/products.js
const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Assurez-vous que le chemin vers votre pool de connexion est correct

// Route pour récupérer tous les produits
router.get('/', async (req, res) => {
  try {
    const result = await pool.query(`
      SELECT
          p.id,
          p.marque,
          p.modele,
          p.stockage,
          p.type,
          p.type_carton,
          p.imei,
          p.quantite,
          p.prix_vente,
          p.prix_achat,      -- Assurez-vous que cette colonne est sélectionnée
          p.status,
          p.date_ajout,
          p.fournisseur_id,  -- Assurez-vous que cette colonne est sélectionnée
          f.nom AS nom_fournisseur -- Jointure pour obtenir le nom du fournisseur
      FROM products p
      LEFT JOIN fournisseurs f ON p.fournisseur_id = f.id
      ORDER BY p.date_ajout DESC
    `);
    res.status(200).json(result.rows);
  } catch (error) {
    console.error('Erreur lors de la récupération des produits:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des produits.' });
  }
});

// Route pour récupérer un produit par ID
router.get('/:id', async (req, res) => {
  const { id } = req.params;
  try {
    const result = await pool.query(`
      SELECT
          p.id,
          p.marque,
          p.modele,
          p.stockage,
          p.type,
          p.type_carton,
          p.imei,
          p.quantite,
          p.prix_vente,
          p.prix_achat,
          p.status,
          p.date_ajout,
          p.fournisseur_id,
          f.nom AS nom_fournisseur
      FROM products p
      LEFT JOIN fournisseurs f ON p.fournisseur_id = f.id
      WHERE p.id = $1
    `, [id]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Produit non trouvé.' });
    }
    res.status(200).json(result.rows[0]);
  } catch (error) {
    console.error('Erreur lors de la récupération du produit par ID:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération du produit.' });
  }
});

// Route pour ajouter plusieurs produits (BATCH INSERT)
router.post('/batch', async (req, res) => {
  const {
    marque, modele, stockage, type, type_carton, imei, // 'imei' est un tableau
    prix_vente, prix_achat, fournisseur_id // ✅ Assurez-vous que ces champs sont présents dans le corps de la requête
  } = req.body;

  // Validation de base pour les champs globaux
  if (!marque || !modele || !type || !prix_vente || !prix_achat || !fournisseur_id || !Array.isArray(imei) || imei.length === 0) {
    return res.status(400).json({ error: 'Tous les champs requis (marque, modèle, type, prix_vente, prix_achat, fournisseur_id) et au moins un IMEI sont nécessaires.' });
  }

  // Si type est 'CARTON' et marque est 'iPhone', alors type_carton est requis
  if (type === 'CARTON' && marque.toLowerCase() === 'iphone' && !type_carton) {
    return res.status(400).json({ error: 'Le type de carton est requis pour les iPhones en carton.' });
  }

  const client = await pool.connect();
  try {
    await client.query('BEGIN'); // Début de la transaction

    const successProducts = [];
    const failedProducts = [];

    for (const singleImei of imei) {
      if (!/^\d{6}$/.test(singleImei)) {
        failedProducts.push({ imei: singleImei, error: 'IMEI doit contenir exactement 6 chiffres.' });
        continue;
      }

      try {
        const result = await client.query(
          `INSERT INTO products (
              marque, modele, stockage, type, type_carton, imei,
              prix_vente, prix_achat, quantite, date_ajout, status, fournisseur_id
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), 'active', $10) RETURNING *`,
          [
            marque, modele, stockage, type, type_carton || null, singleImei,
            prix_vente, prix_achat, 1, fournisseur_id // ✅ prix_achat et fournisseur_id sont insérés ici
          ]
        );
        successProducts.push(result.rows[0]);
      } catch (insertError) {
        if (insertError.code === '23505') { // Code d'erreur pour violation de contrainte unique
          failedProducts.push({ imei: singleImei, error: 'IMEI déjà existant pour cette combinaison produit.' });
        } else if (insertError.code === '23503') { // Code d'erreur pour violation de clé étrangère
          failedProducts.push({ imei: singleImei, error: 'Fournisseur non trouvé.' });
        } else {
          console.error(`Erreur lors de l'insertion de l'IMEI ${singleImei}:`, insertError);
          failedProducts.push({ imei: singleImei, error: `Erreur interne: ${insertError.message}` });
        }
      }
    }

    await client.query('COMMIT'); // Commit si tout s'est bien passé pour les réussites

    if (successProducts.length > 0) {
      if (failedProducts.length === 0) {
        res.status(201).json({ message: 'Tous les produits ont été ajoutés avec succès.', successProducts });
      } else {
        res.status(207).json({ // 207 Multi-Status
          message: 'Certains produits ont été ajoutés avec succès, mais d\'autres ont échoué.',
          successProducts,
          failedProducts
        });
      }
    } else {
      res.status(400).json({ error: 'Aucun produit n\'a pu être ajouté.', failedProducts });
    }

  } catch (transactionError) {
    await client.query('ROLLBACK'); // Rollback en cas d'erreur de transaction
    console.error('Erreur lors de la transaction d\'ajout de produits en lot:', transactionError);
    res.status(500).json({ error: 'Erreur serveur lors de l\'ajout des produits en lot.' });
  } finally {
    client.release();
  }
});


// Route pour mettre à jour un produit existant
router.put('/:id', async (req, res) => {
  const { id } = req.params;
  const {
    marque, modele, stockage, type, type_carton, imei,
    prix_vente, prix_achat, quantite, fournisseur_id
  } = req.body;

  // Le statut sera toujours 'active' lors d'une modification pour le remettre en stock
  const status = 'active'; 

  if (!marque || !modele || !imei || !type || !prix_vente || !prix_achat || !fournisseur_id) {
    return res.status(400).json({ error: 'Tous les champs requis sont nécessaires pour la mise à jour.' });
  }

  // Validation IMEI pour mise à jour
  if (!/^\d{6}$/.test(imei)) {
    return res.status(400).json({ error: 'L\'IMEI doit contenir exactement 6 chiffres.' });
  }

  // Validation du type_carton si applicable
  if (type === 'CARTON' && marque.toLowerCase() === 'iphone' && !type_carton) {
    return res.status(400).json({ error: 'Le type de carton est requis pour les iPhones en carton.' });
  }


  try {
    const result = await pool.query(
      `UPDATE products SET
          marque = $1, modele = $2, stockage = $3, type = $4, type_carton = $5, imei = $6,
          prix_vente = $7, prix_achat = $8, quantite = $9, status = $10, fournisseur_id = $11
       WHERE id = $12 RETURNING *`,
      [
        marque, modele, stockage, type, type_carton || null, imei,
        prix_vente, prix_achat, quantite, status, fournisseur_id, // Utilise le statut 'active' ici
        id
      ]
    );

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Produit non trouvé.' });
    }
    res.status(200).json(result.rows[0]);
  } catch (error) {
    console.error('Erreur lors de la mise à jour du produit:', error);
    if (error.code === '23505') { // Violation de contrainte unique
      return res.status(409).json({ error: 'Un autre produit avec cette combinaison Marque, Modèle, Stockage, Type, Qualité Carton et IMEI existe déjà.' });
    } else if (error.code === '23503') { // Violation de clé étrangère
      return res.status(400).json({ error: 'Fournisseur non trouvé ou invalide.' });
    }
    res.status(500).json({ error: 'Erreur serveur lors de la mise à jour du produit.' });
  }
});

// Route pour supprimer un produit
router.delete('/:id', async (req, res) => {
  const { id } = req.params;
  const client = await pool.connect(); // Obtenir une connexion pour la transaction
  try {
    await client.query('BEGIN'); // Démarrer la transaction

    // Vérifier si le produit est lié à des ventes
    const salesCheck = await client.query('SELECT 1 FROM vente_items WHERE produit_id = $1 LIMIT 1', [id]);
    if (salesCheck.rows.length > 0) {
      await client.query('ROLLBACK'); // Annuler la transaction
      return res.status(409).json({ error: 'Impossible de supprimer ce produit car il est déjà associé à une ou plusieurs ventes. Veuillez d\'abord supprimer les ventes associées.' });
    }

    // Si aucune vente n'est liée, procéder à la suppression
    const result = await client.query('DELETE FROM products WHERE id = $1 RETURNING *', [id]);
    if (result.rows.length === 0) {
      await client.query('ROLLBACK'); // Annuler la transaction
      return res.status(404).json({ error: 'Produit non trouvé.' });
    }
    await client.query('COMMIT'); // Confirmer la transaction
    res.status(200).json({ message: 'Produit supprimé avec succès.', deletedProduct: result.rows[0] });
  } catch (error) {
    console.error('Erreur lors de la suppression du produit:', error);
    await client.query('ROLLBACK'); // S'assurer que la transaction est annulée en cas d'erreur inattendue
    res.status(500).json({ error: 'Erreur serveur lors de la suppression du produit.' });
  } finally {
    client.release(); // Libérer la connexion
  }
});

module.exports = router;
