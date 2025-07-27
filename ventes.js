const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Assurez-vous que le chemin vers db.js est correct
const pdf = require('html-pdf'); // Importation de la bibliothèque html-pdf

// Fonction utilitaire pour formater les montants
const formatAmount = (amount) => {
  if (amount === null || amount === undefined || isNaN(amount)) {
    return 'N/A';
  }
  return parseFloat(amount).toLocaleString('fr-FR', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
};

// Fonction utilitaire pour formater les dates
const formatDate = (dateString) => {
  if (!dateString) return 'N/A';
  try {
    const date = new Date(dateString);
    if (isNaN(date.getTime())) {
      return 'N/A';
    }
    const options = { year: 'numeric', month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' };
    return date.toLocaleDateString('fr-FR', options);
  } catch (e) {
    console.error("Erreur de formatage de date:", e, "Chaîne originale:", dateString);
    return 'N/A';
  }
};


// Route pour récupérer toutes les ventes avec leurs articles et noms de clients
router.get('/', async (req, res) => {
  try {
    const query = `
      SELECT
          v.id AS vente_id,
          v.date_vente,
          v.montant_total,
          v.montant_paye,
          v.statut_paiement,
          v.is_facture_speciale,
          c.nom AS client_nom,
          c.telephone AS client_telephone,
          JSON_AGG(
              JSON_BUILD_OBJECT(
                  'item_id', vi.id,
                  'produit_id', vi.produit_id,
                  'imei', vi.imei,
                  'quantite_vendue', vi.quantite_vendue,
                  'prix_unitaire_vente', vi.prix_unitaire_vente,
                  'prix_unitaire_achat', vi.prix_unitaire_achat,
                  'marque', vi.marque,
                  'modele', vi.modele,
                  'stockage', vi.stockage,
                  'type_carton', vi.type_carton,
                  'type', vi.type,
                  'statut_vente', vi.statut_vente,
                  'is_special_sale_item', vi.is_special_sale_item,
                  'cancellation_reason', vi.cancellation_reason,
                  'nom_fournisseur', f.nom
              )
              ORDER BY vi.id
          ) AS articles
      FROM
          ventes v
      JOIN
          clients c ON v.client_id = c.id
      JOIN
          vente_items vi ON v.id = vi.vente_id
      LEFT JOIN
          products p ON vi.produit_id = p.id
      LEFT JOIN
          fournisseurs f ON p.fournisseur_id = f.id
      GROUP BY
          v.id, c.nom, c.telephone
      ORDER BY
          v.date_vente DESC;
    `;
    const result = await pool.query(query);
    res.status(200).json(result.rows);
  } catch (error) {
    console.error('Erreur lors de la récupération des ventes:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération des ventes.' });
  }
});

// NOUVELLE ROUTE : GET /api/ventes/:id - Récupérer une vente spécifique par ID
router.get('/:id', async (req, res) => {
  const venteId = req.params.id;
  try {
    const query = `
      SELECT
          v.id AS vente_id,
          v.date_vente,
          v.montant_total,
          v.montant_paye,
          v.statut_paiement,
          v.is_facture_speciale,
          c.nom AS client_nom,
          c.telephone AS client_telephone,
          JSON_AGG(
              JSON_BUILD_OBJECT(
                  'item_id', vi.id,
                  'produit_id', vi.produit_id,
                  'imei', vi.imei,
                  'quantite_vendue', vi.quantite_vendue,
                  'prix_unitaire_vente', vi.prix_unitaire_vente,
                  'prix_unitaire_achat', vi.prix_unitaire_achat,
                  'marque', vi.marque,
                  'modele', vi.modele,
                  'stockage', vi.stockage,
                  'type_carton', vi.type_carton,
                  'type', vi.type,
                  'statut_vente', vi.statut_vente,
                  'is_special_sale_item', vi.is_special_sale_item,
                  'cancellation_reason', vi.cancellation_reason,
                  'nom_fournisseur', f.nom
              )
              ORDER BY vi.id
          ) AS articles
      FROM
          ventes v
      JOIN
          clients c ON v.client_id = c.id
      JOIN
          vente_items vi ON v.id = vi.vente_id
      LEFT JOIN
          products p ON vi.produit_id = p.id
      LEFT JOIN
          fournisseurs f ON p.fournisseur_id = f.id
      WHERE
          v.id = $1
      GROUP BY
          v.id, c.nom, c.telephone
      ORDER BY
          v.date_vente DESC;
    `;
    const result = await pool.query(query, [venteId]);
    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Vente non trouvée.' });
    }
    res.status(200).json(result.rows[0]);
  } catch (error) {
    console.error(`Erreur lors de la récupération de la vente ${venteId}:`, error);
    res.status(500).json({ error: 'Erreur serveur lors de la récupération de la vente.' });
  }
});


// Route pour créer une nouvelle vente (gestion des remises)
router.post('/', async (req, res) => {
  const { nom_client, items, montant_paye, client_telephone, is_facture_speciale, montant_negocie } = req.body;
  let clientDb;

  console.log('Backend: Requête POST /ventes reçue.');
  console.log('Backend: Corps de la requête:', req.body);

  if (!nom_client || !items || items.length === 0) {
    console.log('Backend: Erreur 400 - Nom client ou articles manquants.');
    return res.status(400).json({ error: 'Le nom du client et les articles sont requis.' });
  }

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN'); // Début de la transaction
    console.log('Backend: Transaction démarrée.');

    // 1. Récupérer ou créer le client
    let clientId;
    const clientResult = await clientDb.query('SELECT id, telephone FROM clients WHERE nom = $1', [nom_client]);
    if (clientResult.rows.length > 0) {
      clientId = clientResult.rows[0].id;
      console.log('Backend: Client existant trouvé, ID:', clientId);
      if (client_telephone && client_telephone !== clientResult.rows[0].telephone) {
        await clientDb.query('UPDATE clients SET telephone = $1 WHERE id = $2', [client_telephone, clientId]);
        console.log('Backend: Téléphone client mis à jour.');
      }
    } else {
      const newClientResult = await clientDb.query(
        'INSERT INTO clients (nom, telephone) VALUES ($1, $2) RETURNING id',
        [nom_client, client_telephone || null]
      );
      clientId = newClientResult.rows[0].id;
      console.log('Backend: Nouveau client créé, ID:', clientId);
    }

    // 2. Calculer le montant total de la vente et vérifier la disponibilité des produits
    let montantTotal = 0;
    // Si un montant négocié est fourni, utilisez-le, sinon calculez la somme des articles
    if (montant_negocie !== undefined && !isNaN(parseFloat(montant_negocie))) {
        montantTotal = parseFloat(montant_negocie);
    } else {
        for (const item of items) {
            const productResult = await clientDb.query(
                `SELECT id, quantite, prix_vente, prix_achat, marque, modele, type_carton, stockage, type, status, fournisseur_id FROM products
                 WHERE imei = $1 AND marque = $2 AND modele = $3
                 AND (stockage = $4 OR (stockage IS NULL AND $4 IS NULL))
                 AND (type = $5 OR (type IS NULL AND $5 IS NULL))
                 AND (type_carton = $6 OR (type_carton IS NULL AND $6 IS NULL))`,
                [item.imei, item.marque, item.modele, item.stockage, item.type, item.type_carton]
            );

            if (productResult.rows.length === 0) {
                await clientDb.query('ROLLBACK');
                console.log(`Backend: Erreur 404 - Produit avec IMEI "${item.imei}" non trouvé.`);
                return res.status(404).json({ error: `Produit avec IMEI "${item.imei}" et la combinaison spécifiée non trouvé.` });
            }

            const product = productResult.rows[0];
            if (product.status !== 'active') {
                await clientDb.query('ROLLBACK');
                console.log(`Backend: Erreur 400 - Produit avec IMEI "${item.imei}" n'est pas disponible pour la vente (statut: ${product.status}).`);
                return res.status(400).json({ error: `Produit avec IMEI "${item.imei}" n'est pas disponible pour la vente (statut: ${product.status}).` });
            }

            const prixUnitaireVenteFinal = parseFloat(item.prix_unitaire_vente || product.prix_vente);
            const prixUnitaireAchat = parseFloat(product.prix_achat); // RÉCUPÉRATION DU PRIX D'ACHAT

            // NOUVELLE VALIDATION : Le prix de vente ne peut pas être inférieur au prix d'achat
            if (prixUnitaireVenteFinal < prixUnitaireAchat) {
                await clientDb.query('ROLLBACK');
                console.log(`Backend: Erreur 400 - Prix de vente (${prixUnitaireVenteFinal}) inférieur au prix d'achat (${prixUnitaireAchat}) pour IMEI "${item.imei}".`);
                return res.status(400).json({ error: `Le prix de vente (${prixUnitaireVenteFinal}) de l'IMEI "${item.imei}" ne peut pas être inférieur à son prix d'achat (${prixUnitaireAchat}).` });
            }

            if (isNaN(prixUnitaireVenteFinal) || prixUnitaireVenteFinal <= 0) {
                await clientDb.query('ROLLBACK');
                console.log(`Backend: Erreur 400 - Prix de vente final invalide pour IMEI "${item.imei}".`);
                return res.status(400).json({ error: `Le prix de vente final pour l'IMEI "${item.imei}" est invalide ou négatif.` });
            }
            montantTotal += item.quantite_vendue * prixUnitaireVenteFinal;
        }
    }


    const productStatusUpdates = [];
    const saleItems = [];

    for (const item of items) {
      console.log('Backend: Traitement de l\'article:', item.imei);
      const productResult = await clientDb.query(
        `SELECT id, quantite, prix_vente, prix_achat, marque, modele, type_carton, stockage, type, status, fournisseur_id FROM products
         WHERE imei = $1 AND marque = $2 AND modele = $3
         AND (stockage = $4 OR (stockage IS NULL AND $4 IS NULL))
         AND (type = $5 OR (type IS NULL AND $5 IS NULL))
         AND (type_carton = $6 OR (type_carton IS NULL AND $6 IS NULL))`,
        [item.imei, item.marque, item.modele, item.stockage, item.type, item.type_carton]
      );

      const product = productResult.rows[0]; // Already checked for existence above

      const prixUnitaireVenteFinal = parseFloat(item.prix_unitaire_vente || product.prix_vente);
      const prixUnitaireAchat = parseFloat(product.prix_achat);

      productStatusUpdates.push({ id: product.id, newStatus: 'sold' });

      saleItems.push({
        produit_id: product.id,
        imei: item.imei,
        quantite_vendue: item.quantite_vendue,
        prix_unitaire_vente: prixUnitaireVenteFinal,
        prix_unitaire_achat: prixUnitaireAchat,
        marque: product.marque,
        modele: product.modele,
        type_carton: product.type_carton,
        stockage: product.stockage,
        type: product.type,
        statut_vente: 'actif',
        is_special_sale_item: is_facture_speciale, // Utilise la valeur reçue
        cancellation_reason: null
      });
    }
    console.log('Backend: Montant total calculé:', montantTotal);
    console.log('Backend: Articles de vente préparés:', saleItems);

    let statutPaiement = 'en_attente_paiement';
    const parsedMontantPaye = parseFloat(montant_paye);

    if (parsedMontantPaye >= montantTotal) {
      statutPaiement = 'payee_integralement';
    } else if (parsedMontantPaye > 0) {
      statutPaiement = 'paiement_partiel';
    }
    console.log('Backend: Statut de paiement déterminé:', statutPaiement);

    // 3. Insérer la nouvelle vente dans la table `ventes`
    const newSaleResult = await clientDb.query(
      'INSERT INTO ventes (client_id, date_vente, montant_total, montant_paye, statut_paiement, is_facture_speciale) VALUES ($1, NOW(), $2, $3, $4, $5) RETURNING id',
      [clientId, montantTotal, parsedMontantPaye, statutPaiement, is_facture_speciale]
    );
    const nouvelleVenteId = newSaleResult.rows[0].id;
    console.log('Backend: Vente insérée avec succès, ID:', nouvelleVenteId);

    // 4. Insérer les articles de vente dans la table `vente_items`
    for (const item of saleItems) {
      await clientDb.query(
        `INSERT INTO vente_items (vente_id, produit_id, imei, quantite_vendue, prix_unitaire_vente, prix_unitaire_achat, marque, modele, type_carton, stockage, type, statut_vente, is_special_sale_item, cancellation_reason)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)`,
        [
          nouvelleVenteId, item.produit_id, item.imei, item.quantite_vendue,
          item.prix_unitaire_vente, item.prix_unitaire_achat,
          item.marque, item.modele, item.type_carton, item.stockage, item.type,
          item.statut_vente, item.is_special_sale_item, item.cancellation_reason
        ]
      );
    }
    console.log('Backend: Articles de vente insérés.');

    // 5. Mettre à jour le statut des produits dans l'inventaire
    for (const update of productStatusUpdates) {
      await clientDb.query(
        'UPDATE products SET status = $1 WHERE id = $2',
        [update.newStatus, update.id]
      );
    }
    console.log('Backend: Statut des produits mis à jour.');

    // ATTENTION: La logique d'insertion de facture a été supprimée ici.
    // Les factures seront gérées par une route /api/factures dédiée si is_facture_speciale est true.

    await clientDb.query('COMMIT');
    console.log('Backend: Transaction validée (COMMIT).');
    console.log('Backend: Envoi de la réponse JSON au frontend avec venteId:', nouvelleVenteId);
    res.status(201).json({ message: 'Vente enregistrée avec succès!', vente_id: nouvelleVenteId });

  } catch (error) {
    if (clientDb) await clientDb.query('ROLLBACK');
    console.error('Backend: Erreur CRITIQUE lors de l\'enregistrement de la vente:', error);
    res.status(500).json({ error: 'Erreur serveur lors de l\'enregistrement de la vente.' });
  } finally {
    if (clientDb) clientDb.release();
    console.log('Backend: Connexion à la base de données relâchée.');
  }
});

// Route pour annuler un article de vente (et réactiver le produit)
router.post('/cancel-item', async (req, res) => {
  const { venteId, itemId, produitId, imei, quantite, reason } = req.body;
  let clientDb;

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    const itemCheckResult = await clientDb.query(
        'SELECT is_special_sale_item, prix_unitaire_vente, quantite_vendue FROM vente_items WHERE id = $1 AND vente_id = $2',
        [itemId, venteId]
    );

    if (itemCheckResult.rows.length === 0) {
        await clientDb.query('ROLLBACK');
        return res.status(404).json({ error: 'Article de vente non trouvé.' });
    }

    const { is_special_sale_item, prix_unitaire_vente, quantite_vendue } = itemCheckResult.rows[0];

    const updateItemResult = await clientDb.query(
        'UPDATE vente_items SET statut_vente = $1, cancellation_reason = $2 WHERE id = $3 AND vente_id = $4 RETURNING *',
        ['annule', reason, itemId, venteId]
    );

    if (updateItemResult.rows.length === 0) {
        await clientDb.query('ROLLBACK');
        return res.status(404).json({ error: 'Article de vente non trouvé ou déjà annulé.' });
    }

    if (!is_special_sale_item && produitId) {
        await clientDb.query(
            'UPDATE products SET status = $1 WHERE id = $2 AND imei = $3',
            ['active', produitId, imei]
        );
    }

    // Recalculer le montant total de la vente après annulation de l'article
    const recalculatedSaleTotalResult = await clientDb.query(
      `SELECT COALESCE(SUM(vi.prix_unitaire_vente * vi.quantite_vendue), 0) AS new_montant_total
       FROM vente_items vi
       WHERE vi.vente_id = $1 AND vi.statut_vente = 'actif'`, // Seuls les articles actifs comptent
      [venteId]
    );
    const newMontantTotal = parseFloat(recalculatedSaleTotalResult.rows[0].new_montant_total);

    // Récupérer le montant payé actuel pour déterminer le nouveau statut de paiement
    const currentSaleResult = await clientDb.query('SELECT montant_paye FROM ventes WHERE id = $1', [venteId]);
    const currentMontantPaye = parseFloat(currentSaleResult.rows[0].montant_paye);

    let newStatutPaiement = 'en_attente_paiement';
    if (newMontantTotal <= currentMontantPaye) {
      newStatutPaiement = 'payee_integralement';
    } else if (currentMontantPaye > 0) {
      newStatutPaiement = 'paiement_partiel';
    } else if (currentMontantPaye === 0) {
      newStatutPaiement = 'en_attente_paiement';
    }

    // Mettre à jour la vente principale avec le nouveau montant total et statut
    await clientDb.query(
      'UPDATE ventes SET montant_total = $1, statut_paiement = $2 WHERE id = $3',
      [newMontantTotal, newStatutPaiement, venteId]
    );

    // Calculer le nouveau montant_actuel_du pour la facture
    const newMontantActuelDu = newMontantTotal - currentMontantPaye;

    // Mettre à jour le statut de la facture associée
    await clientDb.query(
      'UPDATE factures SET statut_facture = $1, montant_original_facture = $2, montant_actuel_du = $3, montant_paye_facture = $4 WHERE vente_id = $5',
      [newStatutPaiement, newMontantTotal, newMontantActuelDu, currentMontantPaye, venteId]
    );

    // Vérifier si tous les articles de la vente sont maintenant inactifs (annulés/retournés/rendu)
    const saleItemsStatusCheck = await clientDb.query(
      'SELECT COUNT(*) AS total_items, SUM(CASE WHEN statut_vente IN (\'annule\', \'retourne\', \'rendu\') THEN 1 ELSE 0 END) AS inactive_items FROM vente_items WHERE vente_id = $1',
      [venteId]
    );
    const { total_items, inactive_items } = saleItemsStatusCheck.rows[0];

    if (parseInt(inactive_items, 10) === parseInt(total_items, 10)) {
        await clientDb.query(
            'UPDATE ventes SET statut_paiement = \'annulee\' WHERE id = $1',
            [venteId]
        );
        // Mettre à jour également la facture si la vente entière est annulée
        await clientDb.query(
            'UPDATE factures SET statut_facture = \'annulee\' WHERE vente_id = $1',
            [venteId]
        );
    }

    await clientDb.query('COMMIT');
    res.status(200).json({ message: 'Article annulé et produit réactivé si applicable.' });

  } catch (error) {
    if (clientDb) {
      await clientDb.query('ROLLBACK');
    }
    console.error('Erreur lors de l\'annulation de l\'article:', error);
    res.status(500).json({ error: 'Erreur serveur lors de l\'annulation de l\'article.' });
  } finally {
    if (clientDb) {
      clientDb.release();
    }
  }
});

// PUT /api/ventes/:id/update-payment - Mettre à jour le paiement et potentiellement le montant total d'une vente
router.put('/:id/update-payment', async (req, res) => {
  const saleId = req.params.id;
  const { montant_paye, new_total_amount } = req.body;
  let clientDb;

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    const saleResult = await clientDb.query('SELECT montant_total, montant_paye FROM ventes WHERE id = $1 FOR UPDATE', [saleId]); // FOR UPDATE pour verrouiller la ligne
    if (saleResult.rows.length === 0) {
      await clientDb.query('ROLLBACK');
      return res.status(404).json({ error: 'Vente non trouvée.' });
    }
    let currentMontantTotal = parseFloat(saleResult.rows[0].montant_total);
    let currentMontantPaye = parseFloat(saleResult.rows[0].montant_paye);

    const parsedMontantPaye = parseFloat(montant_paye);
    const finalMontantTotal = new_total_amount !== undefined && !isNaN(parseFloat(new_total_amount)) ? parseFloat(new_total_amount) : currentMontantTotal;

    if (isNaN(parsedMontantPaye) || parsedMontantPaye < 0) {
        await clientDb.query('ROLLBACK');
        return res.status(400).json({ error: 'Le montant payé doit être un nombre positif ou zéro.' });
    }
    if (isNaN(finalMontantTotal) || finalMontantTotal <= 0) {
        await clientDb.query('ROLLBACK');
        return res.status(400).json({ error: 'Le montant total négocié doit être un nombre positif.' });
    }

    if (parsedMontantPaye > finalMontantTotal) {
      await clientDb.query('ROLLBACK');
      return res.status(400).json({ error: `Le montant payé (${formatAmount(parsedMontantPaye)}) ne peut pas être supérieur au montant total de la vente (${formatAmount(finalMontantTotal)}).` });
    }
    if (finalMontantTotal < currentMontantPaye && currentMontantPaye > 0) {
        await clientDb.query('ROLLBACK');
        return res.status(400).json({ error: `Le nouveau montant total (${formatAmount(finalMontantTotal)}) ne peut pas être inférieur au montant déjà payé (${formatAmount(currentMontantPaye)}).` });
    }


    let statutPaiement = 'paiement_partiel';
    if (parsedMontantPaye >= finalMontantTotal) {
      statutPaiement = 'payee_integralement';
    } else if (parsedMontantPaye > 0) {
      statutPaiement = 'paiement_partiel';
    } else if (parsedMontantPaye === 0) {
      statutPaiement = 'en_attente_paiement';
    }

    const result = await clientDb.query(
      'UPDATE ventes SET montant_paye = $1, montant_total = $2, statut_paiement = $3 WHERE id = $4 RETURNING *',
      [parsedMontantPaye, finalMontantTotal, statutPaiement, saleId]
    );

    // Calculer le nouveau montant_actuel_du pour la facture
    const newMontantActuelDu = finalMontantTotal - parsedMontantPaye;

    // Mettre à jour le statut de la facture associée dans la table 'factures'
    await clientDb.query(
      'UPDATE factures SET statut_facture = $1, montant_original_facture = $2, montant_actuel_du = $3, montant_paye_facture = $4 WHERE vente_id = $5',
      [statutPaiement, finalMontantTotal, newMontantActuelDu, parsedMontantPaye, saleId]
    );

    await clientDb.query('COMMIT');
    res.status(200).json(result.rows[0]);

  } catch (error) {
    if (clientDb) {
      await clientDb.query('ROLLBACK');
    }
    console.error('Erreur lors de la mise à jour du paiement:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la mise à jour du paiement.' });
  } finally {
    if (clientDb) {
      clientDb.release();
    }
  }
});

// Route pour gérer le retour d'un article
router.post('/return-item', async (req, res) => {
  const { vente_item_id, vente_id, client_nom, imei, reason, produit_id, is_special_sale_item, marque, modele, stockage, type, type_carton } = req.body;

  let clientDb;

  if (!vente_item_id || !vente_id || !imei || !reason || !client_nom) {
    return res.status(400).json({ error: 'Données de retour manquantes ou invalides.' });
  }

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    const itemCheckResult = await clientDb.query(
      'SELECT is_special_sale_item, prix_unitaire_vente, quantite_vendue FROM vente_items WHERE id = $1 AND vente_id = $2',
      [vente_item_id, vente_id]
    );

    if (itemCheckResult.rows.length === 0) {
        await clientDb.query('ROLLBACK');
        return res.status(404).json({ error: 'Article de vente non trouvé.' });
    }

    const { is_special_sale_item, prix_unitaire_vente, quantite_vendue } = itemCheckResult.rows[0];

    // Mettre à jour le statut de l'article de vente à 'retourne'
    const updateItemResult = await clientDb.query(
        'UPDATE vente_items SET statut_vente = $1, cancellation_reason = $2 WHERE id = $3 AND vente_id = $4 RETURNING *',
        ['retourne', reason, vente_item_id, vente_id]
    );

    if (updateItemResult.rows.length === 0) {
        await clientDb.query('ROLLBACK');
        return res.status(404).json({ error: 'Article de vente non trouvé ou déjà retourné.' });
    }

    // Si ce n'est pas un article de facture spéciale, mettre à jour le statut du produit dans l'inventaire
    if (!is_special_sale_item && produit_id) {
        await clientDb.query(
            'UPDATE products SET status = $1 WHERE id = $2 AND imei = $3',
            ['returned', produit_id, imei] // Nouveau statut 'returned'
        );
    }

    // Enregistrer le retour dans la table 'returns'
    const clientResult = await clientDb.query('SELECT id FROM clients WHERE nom = $1', [client_nom]);
    let clientId = clientResult.rows[0]?.id;

    if (!clientId) {
        // Fallback ou gestion d'erreur si le client n'est pas trouvé (devrait être rare)
        console.warn(`Client "${client_nom}" non trouvé pour le retour. Utilisation de 0.`);
        clientId = 0; // Ou gérer comme une erreur si le client est obligatoire
    }

    // Correction: Ajout de vente_id dans l'INSERT INTO returns
    await clientDb.query(
      `INSERT INTO returns (
        vente_item_id, vente_id, client_id, marque, modele, stockage, type, type_carton, imei, reason, return_date, status, product_id, is_special_sale_item
      ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, NOW(), $11, $12, $13)`,
      [
        vente_item_id, vente_id, clientId, marque, modele, stockage, type, type_carton, imei, reason, 'retourne', produit_id, is_special_sale_item
      ]
    );

    // Recalculer le montant total de la vente et le statut de paiement après le retour
    const recalculatedSaleTotalResult = await clientDb.query(
      `SELECT COALESCE(SUM(vi.prix_unitaire_vente * vi.quantite_vendue), 0) AS new_montant_total
       FROM vente_items vi
       WHERE vi.vente_id = $1 AND vi.statut_vente = 'actif'`, // Seuls les articles actifs comptent
      [vente_id]
    );
    const newMontantTotal = parseFloat(recalculatedSaleTotalResult.rows[0].new_montant_total);

    const currentSaleResult = await clientDb.query('SELECT montant_paye FROM ventes WHERE id = $1', [vente_id]);
    const currentMontantPaye = parseFloat(currentSaleResult.rows[0].montant_paye);

    let newStatutPaiement = 'en_attente_paiement';
    if (newMontantTotal <= currentMontantPaye) {
      newStatutPaiement = 'payee_integralement';
    } else if (currentMontantPaye > 0) {
      newStatutPaiement = 'paiement_partiel';
    } else if (currentMontantPaye === 0) {
      newStatutPaiement = 'en_attente_paiement';
    }

    await clientDb.query(
      'UPDATE ventes SET montant_total = $1, statut_paiement = $2 WHERE id = $3',
      [newMontantTotal, newStatutPaiement, vente_id]
    );

    // Calculer le nouveau montant_actuel_du pour la facture
    const newMontantActuelDu = newMontantTotal - currentMontantPaye;

    // Mettre à jour le statut de la facture associée
    await clientDb.query(
      'UPDATE factures SET statut_facture = $1, montant_original_facture = $2, montant_actuel_du = $3, montant_paye_facture = $4 WHERE vente_id = $5',
      [newStatutPaiement, newMontantTotal, newMontantActuelDu, currentMontantPaye, vente_id]
    );

    // Vérifier si tous les articles de la vente sont maintenant inactifs (annulés/retournés/rendu)
    const saleItemsStatusCheck = await clientDb.query(
      'SELECT COUNT(*) AS total_items, SUM(CASE WHEN statut_vente IN (\'annule\', \'retourne\', \'rendu\') THEN 1 ELSE 0 END) AS inactive_items FROM vente_items WHERE vente_id = $1',
      [vente_id]
    );
    const { total_items, inactive_items } = saleItemsStatusCheck.rows[0];

    if (parseInt(inactive_items, 10) === parseInt(total_items, 10)) {
        await clientDb.query(
            'UPDATE ventes SET statut_paiement = \'annulee\' WHERE id = $1',
            [vente_id]
        );
        // Mettre à jour également la facture si la vente entière est annulée
        await clientDb.query(
            'UPDATE factures SET statut_facture = \'annulee\' WHERE vente_id = $1',
            [vente_id]
        );
    }

    await clientDb.query('COMMIT');
    res.status(200).json({ message: 'Article retourné et enregistré avec succès.' });

  } catch (error) {
    if (clientDb) {
      await clientDb.query('ROLLBACK');
    }
    console.error('Erreur lors du retour de l\'article:', error);
    res.status(500).json({ error: 'Erreur serveur lors du retour de l\'article.' });
  } finally {
    if (clientDb) {
      clientDb.release();
    }
  }
});

// Route pour marquer un article comme "rendu" (client a rendu le mobile)
router.post('/mark-as-rendu', async (req, res) => {
  const { vente_item_id, vente_id, imei, reason, produit_id, is_special_sale_item, marque, modele, stockage, type, type_carton, client_nom } = req.body;
  let clientDb;

  if (!vente_item_id || !vente_id || !imei || !reason || !produit_id) {
    return res.status(400).json({ error: 'Données de rendu manquantes ou invalides.' });
  }

  try {
    clientDb = await pool.connect();
    await clientDb.query('BEGIN');

    // 1. Mettre à jour le statut de l'article de vente à 'rendu'
    const updateItemResult = await clientDb.query(
      'UPDATE vente_items SET statut_vente = $1, cancellation_reason = $2 WHERE id = $3 AND vente_id = $4 RETURNING *',
      ['rendu', reason, vente_item_id, vente_id]
    );

    if (updateItemResult.rows.length === 0) {
      await clientDb.query('ROLLBACK');
      return res.status(404).json({ error: 'Article de vente non trouvé ou déjà marqué comme rendu.' });
    }

    // 2. Remettre le produit en 'active' dans la table products, quelle que soit is_special_sale_item
    // C'est la modification clé : suppression de la condition `!is_special_sale_item`
    if (produit_id) {
      await clientDb.query(
        'UPDATE products SET status = $1, quantite = 1, date_ajout = NOW() WHERE id = $2 AND imei = $3', // Ajout de quantite=1 et date_ajout=NOW()
        ['active', produit_id, imei]
      );
    }

    // 3. Recalculer le montant total de la vente et le statut de paiement
    const recalculatedSaleTotalResult = await clientDb.query(
      `SELECT COALESCE(SUM(vi.prix_unitaire_vente * vi.quantite_vendue), 0) AS new_montant_total
       FROM vente_items vi
       WHERE vi.vente_id = $1 AND vi.statut_vente = 'actif'`, // Seuls les articles actifs comptent
      [vente_id]
    );
    const newMontantTotal = parseFloat(recalculatedSaleTotalResult.rows[0].new_montant_total);

    const currentSaleResult = await clientDb.query('SELECT montant_paye FROM ventes WHERE id = $1', [vente_id]);
    const currentMontantPaye = parseFloat(currentSaleResult.rows[0].montant_paye);

    let newStatutPaiement = 'en_attente_paiement';
    if (newMontantTotal <= currentMontantPaye) {
      newStatutPaiement = 'payee_integralement';
    } else if (currentMontantPaye > 0) {
      newStatutPaiement = 'paiement_partiel';
    } else if (newMontantTotal === 0 && currentMontantPaye === 0) { // Cas où la vente est vide et rien n'a été payé
      newStatutPaiement = 'annulee'; // Ou un statut spécifique pour les ventes rendues sans paiement
    }


    // Mettre à jour la vente principale avec le nouveau montant total et statut
    await clientDb.query(
      'UPDATE ventes SET montant_total = $1, statut_paiement = $2 WHERE id = $3',
      [newMontantTotal, newStatutPaiement, vente_id]
    );

    // Calculer le nouveau montant_actuel_du pour la facture
    const newMontantActuelDu = newMontantTotal - currentMontantPaye;

    // 4. Mettre à jour le statut de la facture associée
    await clientDb.query(
      'UPDATE factures SET statut_facture = $1, montant_original_facture = $2, montant_actuel_du = $3, montant_paye_facture = $4 WHERE vente_id = $5',
      [newStatutPaiement, newMontantTotal, newMontantActuelDu, currentMontantPaye, vente_id]
    );

    // Optionnel: Mettre à jour le statut de la vente mère si tous les articles sont rendus/annulés/retournés
    const saleItemsStatusCheck = await clientDb.query(
      'SELECT COUNT(*) AS total_items, SUM(CASE WHEN statut_vente IN (\'annule\', \'retourne\', \'rendu\') THEN 1 ELSE 0 END) AS inactive_items FROM vente_items WHERE vente_id = $1',
      [vente_id]
    );
    const { total_items, inactive_items } = saleItemsStatusCheck.rows[0];

    if (parseInt(inactive_items, 10) === parseInt(total_items, 10)) {
        await clientDb.query(
            'UPDATE ventes SET statut_paiement = \'annulee\' WHERE id = $1', // Ou un autre statut comme 'vente_rendue'
            [vente_id]
        );
        // Mettre à jour également la facture si la vente entière est annulée
        await clientDb.query(
            'UPDATE factures SET statut_facture = \'annulee\' WHERE vente_id = $1',
            [vente_id]
        );
    }

    await clientDb.query('COMMIT');
    res.status(200).json({ message: 'Article marqué comme rendu et remis en stock avec succès.' });

  } catch (error) {
    if (clientDb) {
      await clientDb.query('ROLLBACK');
    }
    console.error('Erreur lors du marquage comme rendu de l\'article:', error);
    res.status(500).json({ error: 'Erreur serveur lors du marquage comme rendu de l\'article.' });
  } finally {
    if (clientDb) {
      clientDb.release();
    }
  }
});


// Route pour générer un PDF de la facture pour une vente donnée
router.get('/:id/pdf', async (req, res) => {
  const venteId = req.params.id;
  let clientDb;

  try {
    clientDb = await pool.connect();

    const saleDetailsQuery = `
      SELECT
          v.id AS vente_id,
          v.date_vente,
          v.montant_total,
          v.montant_paye,
          v.statut_paiement,
          c.nom AS client_nom,
          c.telephone AS client_telephone,
          JSON_AGG(
              JSON_BUILD_OBJECT(
                  'item_id', vi.id,
                  'produit_id', vi.produit_id,
                  'imei', vi.imei,
                  'quantite_vendue', vi.quantite_vendue,
                  'prix_unitaire_vente', vi.prix_unitaire_vente,
                  'prix_unitaire_achat', vi.prix_unitaire_achat,
                  'marque', vi.marque,
                  'modele', vi.modele,
                  'stockage', vi.stockage,
                  'type_carton', vi.type_carton,
                  'type', vi.type,
                  'statut_vente', vi.statut_vente,
                  'nom_fournisseur', f.nom
              )
              ORDER BY vi.id
          ) AS articles
      FROM
          ventes v
      JOIN
          clients c ON v.client_id = c.id
      JOIN
          vente_items vi ON v.id = vi.vente_id
      LEFT JOIN
          products p ON vi.produit_id = p.id
      LEFT JOIN
          fournisseurs f ON p.fournisseur_id = f.id
      WHERE
          v.id = $1
      GROUP BY
          v.id, c.nom, c.telephone;
    `;
    const result = await clientDb.query(saleDetailsQuery, [venteId]); // Correction: utilisation de 'saleDetailsQuery'

    if (result.rows.length === 0) {
      return res.status(404).json({ error: 'Vente non trouvée.' });
    }

    const sale = result.rows[0];
    const balanceDue = sale.montant_total - sale.montant_paye;

    let articlesHtml = sale.articles.map(item => {
      let descriptionParts = [item.marque, item.modele];
      if (item.stockage) {
        descriptionParts.push(`(${item.stockage})`);
      }

      let typeInfo = '';
      if (item.type === 'CARTON' && item.type_carton) {
        typeInfo = `(CARTON ${item.type_carton})`;
      } else if (item.type) {
        typeInfo = `(${item.type})`;
      }
      if (typeInfo) {
        descriptionParts.push(typeInfo);
      }

      const itemDescription = descriptionParts.join(' ');

      return `
        <tr>
          <td style="padding: 8px; border: 1px solid #e0e0e0; font-size: 11px;">${itemDescription}</td>
          <td style="padding: 8px; border: 1px solid #e0e0e0; font-size: 11px;">${item.imei}</td>
          <td style="padding: 8px; border: 1px solid #e0e0e0; text-align: right; font-size: 11px;">${item.quantite_vendue}</td>
          <td style="padding: 8px; border: 1px solid #e0e0e0; text-align: right; font-size: 11px;">${formatAmount(item.prix_unitaire_vente)}</td>
          <td style="padding: 8px; border: 1px solid #e0e0e0; text-align: right; font-size: 11px;">${formatAmount(item.prix_unitaire_vente * item.quantite_vendue)}</td>
        </tr>
      `;
    }).join('');

    const htmlContent = `
      <div style="font-family: 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #333; padding: 20px;">
        <div style="text-align: center; margin-bottom: 20px;">
          <h1 style="color: #007AFF; font-family: 'SF Pro Display', 'Helvetica Neue', Helvetica, Arial, sans-serif; font-size: 24px;">Facture de Vente</h1>
        </div>

        <table style="width: 100%; margin-bottom: 40px; margin-top: 20px;">
          <tr>
            <td style="vertical-align: top; width: 50%; text-align: left;">
              <p style="font-size: 14px;"><strong>Numéro de Vente:</strong> ${sale.vente_id}</p>
              <p style="font-size: 14px;"><strong>Date de Vente:</strong> ${formatDate(sale.date_vente)}</p>
              <p style="font-size: 14px;"><strong>Client:</strong> ${sale.client_nom}</p>
              <p style="font-size: 14px;"><strong>Téléphone:</strong> ${sale.client_telephone || 'N/A'}</p>
            </td>
            <td style="vertical-align: top; width: 50%; text-align: right;">
              <h2 style="color: #007AFF; font-family: 'SF Pro Display', 'Helvetica Neue', Helvetica, Arial, sans-serif; font-size: 18px; margin-bottom: 5px;">[ALY MEGA STORE]</h2>
              <p style="font-size: 12px;">Tél : [79 79 83 77]</p>
              <p style="font-size: 12px;">Adresse : [Halle de Bamako]</p>
            </td>
          </tr>
        </table>

        <h2 style="color: #007AFF; font-family: 'SF Pro Display', 'Helvetica Neue', Helvetica, Arial, sans-serif; font-size: 18px; margin-bottom: 15px;">Articles Vendus:</h2>
        <table style="width: 100%; border-collapse: collapse; margin-bottom: 20px; font-family: 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #333;">
          <thead>
            <tr style="background-color: #e0f2ff; color: #007AFF;">
              <th style="padding: 10px; border: 1px solid #a0d9ff; text-align: left; font-size: 12px;">Article</th>
              <th style="padding: 10px; border: 1px solid #a0d9ff; text-align: left; font-size: 12px;">IMEI</th>
              <th style="padding: 10px; border: 1px solid #a0d9ff; text-align: right; font-size: 12px;">Qté</th>
              <th style="padding: 10px; border: 1px solid #a0d9ff; text-align: right; font-size: 12px;">P.Unit</th>
              <th style="padding: 10px; border: 1px solid #e0e0e0; text-align: right; font-size: 12px;">Montant</th>
            </tr>
          </thead>
          <tbody>
            ${articlesHtml}
          </tbody>
        </table>

        <div style="text-align: right; font-family: 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, sans-serif; color: #333; margin-top: 20px;">
          <p style="font-size: 16px;"><strong>Montant Total:</strong> ${formatAmount(sale.montant_total)} CFA</p>
          <p style="font-size: 16px;"><strong>Montant Payé:</strong> ${formatAmount(sale.montant_paye)} CFA</p>
          <p style="font-size: 18px; font-weight: bold; color: #D9534F;">Balance Due: ${formatAmount(balanceDue)} CFA</p>
        </div>

        <p style="text-align: center; margin-top: 30px; font-family: 'SF Pro Text', 'Helvetica Neue', Helvetica, Arial, sans-serif; font-style: italic; color: #777;">Merci pour votre achat!</p>
      </div>
    `;

    const options = {
      format: 'A4',
      orientation: 'portrait',
      border: '1cm',
      quality: '75',
    };

    pdf.create(htmlContent, options).toBuffer((err, buffer) => {
      if (err) {
        console.error('Erreur lors de la création du PDF:', err);
        return res.status(500).json({ error: 'Erreur lors de la génération du PDF.' });
      }
      res.writeHead(200, {
        'Content-Type': 'application/pdf',
        'Content-Disposition': `attachment; filename=facture_${venteId}.pdf`,
        'Content-Length': buffer.length
      });
      res.end(buffer);
    });

  } catch (error) {
    console.error('Erreur lors de la génération du PDF de la facture:', error);
    res.status(500).json({ error: 'Erreur serveur lors de la génération du PDF.' });
  } finally {
    if (clientDb) clientDb.release();
  }
});

module.exports = router;