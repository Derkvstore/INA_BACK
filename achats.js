// achats.js
const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Importez le pool centralisé

// Route pour obtenir tous les achats (existants)
router.get('/', async (req, res) => {
  try {
    const result = await pool.query('SELECT * FROM achats ORDER BY date_achat DESC');
    res.status(200).json(result.rows);
  } catch (err) {
    console.error('Erreur lors de la récupération des achats:', err);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des achats.' });
  }
});

// Route pour ajouter un nouvel achat standard (qui va dans le stock)
router.post('/', async (req, res) => {
  const { marque, modele, stockage, type_carton, type, imei, prix_achat, quantite } = req.body;
  if (!marque || !modele || !imei || prix_achat === undefined || !quantite) {
    return res.status(400).json({ error: 'Marque, modèle, IMEI, prix d\'achat et quantité sont requis.' });
  }

  let clientDb;
  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    // Insérer l'achat dans la table 'achats'
    const achatResult = await clientDb.query(
      `INSERT INTO achats (
         date_achat, marque, modele, stockage, type_carton, type, imei, prix_achat, quantite, statut_achat_vente
       ) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, $8, 'stock_normal') RETURNING *`,
      [marque, modele, stockage, type_carton, type, imei, prix_achat, quantite]
    );

    // Mettre à jour le stock du produit (ou l'insérer s'il n'existe pas)
    const productExists = await clientDb.query(
      `SELECT id, quantite FROM products
       WHERE imei = $1 AND marque = $2 AND modele = $3
       AND (stockage = $4 OR (stockage IS NULL AND $4 IS NULL))
       AND (type_carton = $5 OR (type_carton IS NULL AND $5 IS NULL))
       AND (type = $6 OR (type IS NULL AND $6 IS NULL))`,
      [imei, marque, modele, stockage, type_carton, type]
    );

    if (productExists.rows.length > 0) {
      const productId = productExists.rows[0].id;
      const newQuantity = productExists.rows[0].quantite + quantite;
      await clientDb.query(
        'UPDATE products SET quantite = $1 WHERE id = $2',
        [newQuantity, productId]
      );
    } else {
      await clientDb.query(
        `INSERT INTO products (marque, modele, stockage, type_carton, type, imei, prix_achat, prix_vente, quantite)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9)`,
        [marque, modele, stockage, type_carton, type, imei, prix_achat, prix_achat * 1.2, quantite] // Exemple prix_vente = prix_achat * 1.2
      );
    }

    await clientDb.query('COMMIT');
    res.status(201).json({ message: 'Achat enregistré et stock mis à jour.', achat: achatResult.rows[0] });

  } catch (err) {
    if (clientDb) await clientDb.query('ROLLBACK');
    console.error('Erreur lors de l\'ajout de l\'achat:', err);
    res.status(500).json({ error: 'Erreur serveur lors de l\'ajout de l\'achat.' });
  } finally {
    if (clientDb) clientDb.release();
  }
});


// NOUVELLE ROUTE : Enregistrer un achat spécial directement lié à une vente
router.post('/vente-speciale', async (req, res) => {
  const {
    imei, marque, modele, stockage, type, type_carton,
    prix_achat,
    prix_vente_unitaire,
    nom_client, client_telephone,
    montant_paye_client
  } = req.body;

  let clientDb;

  if (!imei || !marque || !modele || !nom_client || prix_vente_unitaire === undefined) {
    return res.status(400).json({ error: 'IMEI, marque, modèle, prix de vente et nom du client sont requis.' });
  }

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    // 1. Récupérer ou créer le client
    let clientId;
    const clientResult = await clientDb.query('SELECT id, telephone FROM clients WHERE nom = $1', [nom_client]);
    if (clientResult.rows.length > 0) {
      clientId = clientResult.rows[0].id;
      if (client_telephone && client_telephone !== clientResult.rows[0].telephone) {
        await clientDb.query('UPDATE clients SET telephone = $1 WHERE id = $2', [client_telephone, clientId]);
      }
    } else {
      const newClientResult = await clientDb.query(
        'INSERT INTO clients (nom, telephone) VALUES ($1, $2) RETURNING id',
        [nom_client, client_telephone || null]
      );
      clientId = newClientResult.rows[0].id;
    }

    // 2. Enregistrer l'achat spécial (sans affecter le stock principal de 'products')
    const newPurchaseResult = await clientDb.query(
      `INSERT INTO achats (
         date_achat, marque, modele, stockage, type, type_carton, imei,
         prix_achat, quantite, statut_achat_vente
       ) VALUES (NOW(), $1, $2, $3, $4, $5, $6, $7, 1, 'vente_speciale') RETURNING id`,
      [marque, modele, stockage, type, type_carton, imei, prix_achat || 0]
    );
    const achatId = newPurchaseResult.rows[0].id;

    // 3. Créer la vente associée
    const montantTotalVente = prix_vente_unitaire;
    let statutPaiementVente = 'en_attente_paiement';
    if (montant_paye_client >= montantTotalVente) {
      statutPaiementVente = 'payee_integralement';
    } else if (montant_paye_client > 0) {
      statutPaiementVente = 'paiement_partiel';
    }

    const newSaleResult = await clientDb.query(
      'INSERT INTO ventes (client_id, date_vente, montant_total, montant_paye, statut_paiement) VALUES ($1, NOW(), $2, $3, $4) RETURNING id',
      [clientId, montantTotalVente, montant_paye_client || 0, statutPaiementVente]
    );
    const venteId = newSaleResult.rows[0].id;

    // 4. Insérer l'article de vente dans `vente_items`
    await clientDb.query(
      `INSERT INTO vente_items (
         vente_id, imei, quantite_vendue, prix_unitaire_vente,
         marque, modele, stockage, type, type_carton,
         statut_vente, is_special_sale_item, source_achat_id
       ) VALUES ($1, $2, 1, $3, $4, $5, $6, $7, $8, 'actif', TRUE, $9)`,
      [venteId, imei, prix_vente_unitaire, marque, modele, stockage, type, type_carton, achatId]
    );

    // 5. Mettre à jour l'achat pour inclure l'ID de la vente
    await clientDb.query(
      'UPDATE achats SET vente_id = $1 WHERE id = $2',
      [venteId, achatId]
    );

    await clientDb.query('COMMIT');
    res.status(201).json({ message: 'Vente spéciale enregistrée avec succès!', venteId, achatId });

  } catch (error) {
    if (clientDb) await clientDb.query('ROLLBACK');
    console.error('Erreur lors de l\'enregistrement de la vente spéciale:', error);
    res.status(500).json({ error: 'Erreur serveur lors de l\'enregistrement de la vente spéciale.' });
  } finally {
    if (clientDb) clientDb.release();
  }
});


module.exports = router;