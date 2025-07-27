// backend/returns.js
const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Assurez-vous que le chemin est correct

// Route pour récupérer tous les mobiles retournés
router.get('/', async (req, res) => {
  try {
    const query = `
      SELECT
          r.id AS return_id,
          r.vente_item_id,
          r.client_id,
          r.imei,
          r.reason,
          r.return_date,
          r.status, -- 'retourne' ou 'sent_to_supplier' (VARCHAR)
          r.product_id,
          r.is_special_sale_item,
          r.source_achat_id,
          r.marque,
          r.modele,
          r.stockage,
          r.type,
          r.type_carton,
          c.nom AS client_nom,
          c.telephone AS client_telephone
      FROM
          returns r
      JOIN
          clients c ON r.client_id = c.id
      ORDER BY
          r.return_date DESC;
    `;
    const result = await pool.query(query);
    res.status(200).json(result.rows);
  } catch (error) {
    console.error('Erreur lors de la récupération des retours:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des retours.' });
  }
});

// ✅ MISE À JOUR : Route pour envoyer les mobiles sélectionnés au fournisseur (batch)
router.post('/send-to-supplier-batch', async (req, res) => {
  const { return_ids } = req.body;
  let clientDb;

  if (!Array.isArray(return_ids) || return_ids.length === 0) {
    return res.status(400).json({ error: 'Une liste d\'IDs de retour est requise.' });
  }

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN'); // Début de la transaction

    const successCount = 0;
    const failedIds = [];

    for (const returnId of return_ids) {
      try {
        // 1. Récupérer les détails de l'entrée de retour depuis la table 'returns'
        // Utilisation de FOR UPDATE pour verrouiller la ligne et éviter les conflits concurrents
        const returnEntryResult = await clientDb.query(
          `SELECT
              r.id, r.vente_item_id, r.client_id, r.imei, r.reason, r.status, r.product_id,
              r.is_special_sale_item, r.source_achat_id, r.marque, r.modele, r.stockage,
              r.type, r.type_carton
           FROM returns r WHERE r.id = $1 FOR UPDATE`,
          [returnId]
        );

        if (returnEntryResult.rows.length === 0) {
          failedIds.push({ id: returnId, error: 'Entrée de retour non trouvée.' });
          continue;
        }

        const returnEntry = returnEntryResult.rows[0];

        // Vérifier le statut actuel du retour (doit être 'retourne' pour être envoyé)
        if (returnEntry.status !== 'retourne') {
          failedIds.push({ id: returnId, error: `Statut invalide: ${returnEntry.status}. Seuls les mobiles 'retourne' peuvent être envoyés.` });
          continue;
        }

        // 2. Mettre à jour le statut dans la table 'returns' à 'sent_to_supplier' (VARCHAR)
        await clientDb.query(
          `UPDATE returns SET status = $1 WHERE id = $2`,
          ['sent_to_supplier', returnId]
        );

        // 3. Insérer une entrée dans la table 'remplacer' avec les colonnes de VOTRE SCHEMA
        await clientDb.query(
          `INSERT INTO remplacer (
              return_id, marque, modele, stockage, type, type_carton, imei,
              date_sent_to_supplier, is_special_sale_item, source_achat_id, resolution_status
           ) VALUES ($1, $2, $3, $4, $5, $6, $7, NOW(), $8, $9, $10)`,
          [
            returnEntry.id,          // return_id de l'entrée returns
            returnEntry.marque,
            returnEntry.modele,
            returnEntry.stockage,
            returnEntry.type,
            returnEntry.type_carton,
            returnEntry.imei,
            returnEntry.is_special_sale_item,
            returnEntry.source_achat_id,
            'PENDING' // Statut de résolution par défaut pour 'remplacer'
          ]
        );

        successCount++;

      } catch (itemError) {
        console.error(`Erreur lors du traitement du retour ID ${returnId}:`, itemError);
        failedIds.push({ id: returnId, error: `Erreur interne: ${itemError.message}` });
      }
    }

    await clientDb.query('COMMIT'); // Commit la transaction

    if (failedIds.length === 0) {
      res.status(200).json({ message: `${successCount} mobile(s) envoyé(s) au fournisseur avec succès.` });
    } else {
      res.status(207).json({ // Multi-Status pour indiquer succès partiel
        message: `${successCount} mobile(s) envoyé(s) avec succès, mais ${failedIds.length} ont échoué.`,
        failedItems: failedIds
      });
    }

  } catch (transactionError) {
    if (clientDb) await clientDb.query('ROLLBACK'); // Rollback en cas d'erreur de transaction
    console.error('Erreur de transaction lors de l\'envoi groupé au fournisseur:', transactionError);
    res.status(500).json({ error: 'Erreur serveur lors de l\'envoi groupé au fournisseur.' });
  } finally {
    if (clientDb) clientDb.release();
  }
});


module.exports = router;
