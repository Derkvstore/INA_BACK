// backend/routes/remplacements.js
const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Assurez-vous que le chemin est correct

// Route pour obtenir tous les mobiles dans la table 'remplacer'
router.get('/', async (req, res) => {
  try {
    const query = `
      SELECT
        id,
        return_id,
        marque,
        modele,
        stockage,
        type,
        type_carton,
        imei,
        date_sent_to_supplier,
        is_special_sale_item,
        source_achat_id,
        resolution_status,
        received_date,
        replacement_product_id
      FROM
        remplacer
      ORDER BY
        date_sent_to_supplier DESC;
    `;
    const result = await pool.query(query);
    res.status(200).json(result.rows);
  } catch (error) {
    console.error('Erreur lors de la récupération des remplacements:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des remplacements.' });
  }
});

// Nouvelle route pour gérer la réception d'un mobile du fournisseur (réparé ou remplacé)
router.post('/receive-from-supplier', async (req, res) => {
  const {
    remplacer_id,
    resolution_type, // 'repaired' ou 'replaced'
    new_product_details // Si 'replaced', contient { marque, modele, stockage, type, type_carton, imei, prix_achat, prix_vente }
  } = req.body;

  let clientDb;

  if (!remplacer_id || !resolution_type) {
    return res.status(400).json({ error: 'ID de remplacement et type de résolution sont requis.' });
  }

  if (resolution_type === 'replaced' && (!new_product_details || !new_product_details.imei || !new_product_details.marque || !new_product_details.modele)) {
    return res.status(400).json({ error: 'Détails du nouveau produit manquants pour un remplacement.' });
  }

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    let replacementProductId = null;

    if (resolution_type === 'repaired') {
      // Si le mobile est réparé, remettez l'IMEI original en stock
      // 1. Trouver l'ID du produit original associé à cette entrée de remplacement
      const originalProductQueryResult = await clientDb.query(
        `SELECT r.product_id
         FROM remplacer repl
         JOIN returns r ON repl.return_id = r.id
         WHERE repl.id = $1`,
        [remplacer_id]
      );

      if (originalProductQueryResult.rows.length === 0 || !originalProductQueryResult.rows[0].product_id) {
        await clientDb.query('ROLLBACK');
        return res.status(404).json({ error: 'Produit original non trouvé pour cette entrée de remplacement (réparé).' });
      }
      const originalProductId = originalProductQueryResult.rows[0].product_id;

      // 2. Mettre à jour le statut du produit original à 'active', sa quantité à 1 ET SA DATE D'AJOUT À MAINTENANT
      await clientDb.query(
        `UPDATE products SET status = 'active', quantite = 1, date_ajout = NOW() WHERE id = $1`,
        [originalProductId]
      );

    } else if (resolution_type === 'replaced') {
      // Si le mobile est remplacé, insérez un tout nouveau produit dans la table 'products'
      const { marque, modele, stockage, type, type_carton, imei, prix_achat, prix_vente } = new_product_details;

      // Vérifier si le nouvel IMEI existe déjà (pour éviter les doublons)
      const existingProduct = await clientDb.query('SELECT id FROM products WHERE imei = $1', [imei]);
      if (existingProduct.rows.length > 0) {
        await clientDb.query('ROLLBACK');
        return res.status(400).json({ error: `Le nouvel IMEI "${imei}" existe déjà dans le stock.` });
      }

      // L'insertion utilise déjà NOW() pour date_ajout, ce qui est correct
      const insertProductResult = await clientDb.query(
        `INSERT INTO products (
          marque, modele, stockage, type_carton, type, imei,
          quantite, prix_achat, prix_vente, date_ajout, status
        ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, NOW(), $10) RETURNING id`,
        [
          marque,
          modele,
          stockage || null,
          type_carton || null,
          type || null,
          imei,
          1, // Quantité 1 pour le nouveau produit
          prix_achat || 0,
          prix_vente || 0,
          'active' // Le nouveau produit est actif par défaut
        ]
      );
      replacementProductId = insertProductResult.rows[0].id;
    }

    // 3. Mettre à jour l'entrée dans la table 'remplacer'
    const updateRemplacerResult = await clientDb.query(
      `UPDATE remplacer
       SET resolution_status = $1,
           received_date = NOW(),
           replacement_product_id = $2
       WHERE id = $3 AND resolution_status = 'PENDING'
       RETURNING *`,
      [
        resolution_type.toUpperCase(), // Stocke en majuscules (REPAIRED ou REPLACED)
        replacementProductId,
        remplacer_id
      ]
    );

    if (updateRemplacerResult.rowCount === 0) {
      await clientDb.query('ROLLBACK');
      return res.status(404).json({ error: 'Entrée de remplacement non trouvée ou déjà résolue.' });
    }

    await clientDb.query('COMMIT');
    res.status(200).json({
      message: `Mobile marqué comme ${resolution_type === 'repaired' ? 'réparé' : 'remplacé'} par le fournisseur.`,
      remplacerEntry: updateRemplacerResult.rows[0],
      newProductId: replacementProductId
    });

  } catch (error) {
    if (clientDb) {
      await clientDb.query('ROLLBACK');
    }
    console.error('Erreur lors de la réception du mobile du fournisseur:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la réception du mobile du fournisseur.' });
  } finally {
    if (clientDb) {
      clientDb.release();
    }
  }
});

// Route pour obtenir les statistiques du tableau de bord
router.get('/dashboard-stats', async (req, res) => {
  try {
    const client = await pool.connect();

    // Compter les mobiles de type 'CARTON' avec le statut 'active'
    const cartonsResult = await client.query(
      `SELECT COALESCE(SUM(quantite), 0) AS total_cartons FROM products WHERE type = 'CARTON' AND status = 'active';`
    );
    const totalCartons = parseInt(cartonsResult.rows[0].total_cartons, 10);

    // Compter les mobiles de type 'ARRIVAGE' avec le statut 'active'
    const arrivageResult = await client.query(
      `SELECT COALESCE(SUM(quantite), 0) AS total_arrivage FROM products WHERE type = 'ARRIVAGE' AND status = 'active';`
    );
    const totalArrivage = parseInt(arrivageResult.rows[0].total_arrivage, 10);

    // Compter les mobiles vendus (statut 'actif' ou 'remplace' dans vente_items)
    const ventesResult = await client.query(
      `SELECT COUNT(*) AS total_ventes FROM vente_items WHERE statut_vente IN ('actif', 'remplace');`
    );
    const totalVentes = parseInt(ventesResult.rows[0].total_ventes, 10);

    // Compter les mobiles remplacés (dans la table 'remplacer')
    const remplacerResult = await client.query(
      `SELECT COUNT(*) AS total_remplacer FROM remplacer;`
    );
    const totalRemplacer = parseInt(remplacerResult.rows[0].total_remplacer, 10);

    client.release(); // Libérer le client après utilisation

    res.status(200).json({
      totalCartons,
      totalArrivage,
      totalVentes,
      totalRemplacer
    });

  } catch (err) {
    console.error('Erreur lors de la récupération des statistiques du tableau de bord:', err);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des statistiques du tableau de bord.' });
  }
});

module.exports = router;
