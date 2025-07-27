const express = require('express');
const router = express.Router();
const { pool } = require('./db'); // Assurez-vous que le chemin est correct

// Helper pour générer un numéro de facture unique (simple, peut être plus complexe)
async function generateInvoiceNumber(clientDb) {
    const today = new Date();
    const year = today.getFullYear();
    const month = String(today.getMonth() + 1).padStart(2, '0');
    const day = String(today.getDate()).padStart(2, '0');

    // Compter les factures du jour pour un suffixe
    const countResult = await clientDb.query(
        `SELECT COUNT(*) FROM factures WHERE DATE(date_facture) = CURRENT_DATE`
    );
    const count = parseInt(countResult.rows[0].count, 10) + 1;
    const suffix = String(count).padStart(3, '0');

    return `INV-${year}${month}${day}-${suffix}`;
}

// POST /api/factures - Créer une nouvelle facture pour une vente donnée
router.post('/', async (req, res) => {
    const { vente_id, observation } = req.body;
    let clientDb;

    if (!vente_id) {
        return res.status(400).json({ error: 'L\'ID de la vente est requis pour créer une facture.' });
    }

    try {
        clientDb = await pool.connect();
        await clientDb.query('BEGIN');

        // Vérifier si une facture existe déjà pour cette vente
        const existingInvoice = await clientDb.query(
            'SELECT id FROM factures WHERE vente_id = $1',
            [vente_id]
        );
        if (existingInvoice.rows.length > 0) {
            await clientDb.query('ROLLBACK');
            return res.status(409).json({ error: 'Une facture existe déjà pour cette vente.' });
        }

        // Récupérer les détails de la vente
        const saleResult = await clientDb.query(
            'SELECT montant_total, montant_paye, statut_paiement FROM ventes WHERE id = $1',
            [vente_id]
        );
        if (saleResult.rows.length === 0) {
            await clientDb.query('ROLLBACK');
            return res.status(404).json({ error: 'Vente non trouvée.' });
        }
        const sale = saleResult.rows[0];

        const numeroFacture = await generateInvoiceNumber(clientDb);

        // Insérer la nouvelle facture
        const newInvoiceResult = await clientDb.query(
            `INSERT INTO factures (
                vente_id, numero_facture, montant_original_facture, montant_actuel_du,
                montant_paye_facture, statut_facture, observation
            ) VALUES ($1, $2, $3, $4, $5, $6, $7) RETURNING *`,
            [
                vente_id,
                numeroFacture,
                sale.montant_total, // Initialement, le montant dû est le montant total - montant payé
                sale.montant_total - sale.montant_paye,
                sale.montant_paye,
                sale.statut_paiement === 'payee_integralement' ? 'payee_integralement' : (sale.statut_paiement === 'paiement_partiel' ? 'paiement_partiel' : 'creee'), // Statut initial basé sur la vente
                observation || null
            ]
        );

        await clientDb.query('COMMIT');
        res.status(201).json({ message: 'Facture créée avec succès.', invoice: newInvoiceResult.rows[0] });

    } catch (error) {
        if (clientDb) await clientDb.query('ROLLBACK');
        console.error('Erreur lors de la création de la facture:', error);
        res.status(500).json({ error: 'Erreur serveur lors de la création de la facture.' });
    } finally {
        if (clientDb) clientDb.release();
    }
});

// GET /api/factures - Récupérer toutes les factures
router.get('/', async (req, res) => {
    try {
        const query = `
            SELECT
                f.id AS facture_id,
                f.numero_facture,
                f.date_facture,
                f.montant_original_facture,
                f.montant_actuel_du,
                f.montant_paye_facture,
                f.statut_facture,
                f.date_annulation,
                f.raison_annulation,
                f.date_dernier_retour,
                f.montant_rembourse,
                f.observation,
                v.id AS vente_id,
                v.date_vente,
                v.montant_total AS vente_montant_total,
                v.montant_paye AS vente_montant_paye,
                v.statut_paiement AS vente_statut_paiement,
                c.nom AS client_nom,
                c.telephone AS client_telephone
            FROM
                factures f
            JOIN
                ventes v ON f.vente_id = v.id
            JOIN
                clients c ON v.client_id = c.id
            ORDER BY
                f.date_facture DESC;
        `;
        const result = await pool.query(query);
        res.status(200).json(result.rows);
    } catch (error) {
        console.error('Erreur lors de la récupération des factures:', error);
        res.status(500).json({ error: 'Erreur serveur lors de la récupération des factures.' });
    }
});

// GET /api/factures/:id - Récupérer une facture spécifique
router.get('/:id', async (req, res) => {
    const { id } = req.params;
    try {
        const query = `
            SELECT
                f.id AS facture_id,
                f.numero_facture,
                f.date_facture,
                f.montant_original_facture,
                f.montant_actuel_du,
                f.montant_paye_facture,
                f.statut_facture,
                f.date_annulation,
                f.raison_annulation,
                f.date_dernier_retour,
                f.montant_rembourse,
                f.observation,
                v.id AS vente_id,
                v.date_vente,
                v.montant_total AS vente_montant_total,
                v.montant_paye AS vente_montant_paye,
                v.statut_paiement AS vente_statut_paiement,
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
                        'source_achat_id', vi.source_achat_id,
                        'cancellation_reason', vi.cancellation_reason,
                        'nom_fournisseur', fo.nom
                    )
                    ORDER BY vi.id
                ) AS articles_vendus
            FROM
                factures f
            JOIN
                ventes v ON f.vente_id = v.id
            JOIN
                clients c ON v.client_id = c.id
            LEFT JOIN
                vente_items vi ON v.id = vi.vente_id
            LEFT JOIN
                products p ON vi.produit_id = p.id
            LEFT JOIN
                fournisseurs fo ON p.fournisseur_id = fo.id
            WHERE
                f.id = $1
            GROUP BY
                f.id, v.id, c.id;
        `;
        const result = await pool.query(query, [id]);
        if (result.rows.length === 0) {
            return res.status(404).json({ error: 'Facture non trouvée.' });
        }
        res.status(200).json(result.rows[0]);
    } catch (error) {
        console.error('Erreur lors de la récupération de la facture par ID:', error);
        res.status(500).json({ error: 'Erreur serveur lors de la récupération de la facture.' });
    }
});


// PUT /api/factures/:id/payment - Mettre à jour le paiement d'une facture et potentiellement le montant total
router.put('/:id/payment', async (req, res) => {
    const { id } = req.params;
    const { montant_paye_facture, new_total_amount } = req.body; // AJOUTÉ: new_total_amount
    let clientDb;

    if (montant_paye_facture === undefined || isNaN(parseFloat(montant_paye_facture)) || parseFloat(montant_paye_facture) < 0) {
        return res.status(400).json({ error: 'Le montant payé doit être un nombre positif ou zéro.' });
    }

    try {
        clientDb = await pool.connect();
        await clientDb.query('BEGIN');

        const invoiceResult = await clientDb.query(
            'SELECT vente_id, montant_original_facture, montant_actuel_du, montant_paye_facture, statut_facture FROM factures WHERE id = $1 FOR UPDATE', // FOR UPDATE pour verrouiller la ligne
            [id]
        );
        if (invoiceResult.rows.length === 0) {
            await clientDb.query('ROLLBACK');
            return res.status(404).json({ error: 'Facture non trouvée.' });
        }
        const invoice = invoiceResult.rows[0];

        if (invoice.statut_facture === 'annulee' || invoice.statut_facture === 'retour_total') {
            await clientDb.query('ROLLBACK');
            return res.status(400).json({ error: `Impossible de modifier le paiement d'une facture ${invoice.statut_facture}.` });
        }

        const newMontantPayeFacture = parseFloat(montant_paye_facture);
        // DÉBUT MODIFICATION POUR NÉGOCIATION POST-FACTURE
        let finalMontantOriginalFacture = parseFloat(invoice.montant_original_facture);
        if (new_total_amount !== undefined && !isNaN(parseFloat(new_total_amount))) {
            finalMontantOriginalFacture = parseFloat(new_total_amount);
            if (finalMontantOriginalFacture < 0) { // Assurez-vous que le montant total n'est pas négatif
                await clientDb.query('ROLLBACK');
                return res.status(400).json({ error: 'Le nouveau montant total ne peut pas être négatif.' });
            }
            if (newMontantPayeFacture > finalMontantOriginalFacture) {
                await clientDb.query('ROLLBACK');
                return res.status(400).json({ error: 'Le montant payé ne peut pas dépasser le nouveau montant total.' });
            }
        }
        // FIN MODIFICATION POUR NÉGOCIATION POST-FACTURE

        const newMontantActuelDu = finalMontantOriginalFacture - newMontantPayeFacture;
        let newStatutFacture = invoice.statut_facture;

        if (newMontantActuelDu <= 0) { // Si le montant dû est zéro ou négatif
            newStatutFacture = 'payee_integralement';
        } else if (newMontantPayeFacture > 0) {
            newStatutFacture = 'paiement_partiel';
        } else {
            newStatutFacture = 'creee'; // Ou 'en_attente_paiement' si vous avez ce statut
        }

        const updateInvoiceResult = await clientDb.query(
            `UPDATE factures
             SET montant_paye_facture = $1, montant_actuel_du = $2, statut_facture = $3, montant_original_facture = $4
             WHERE id = $5 RETURNING *`,
            [newMontantPayeFacture, newMontantActuelDu, newStatutFacture, finalMontantOriginalFacture, id] // AJOUTÉ: finalMontantOriginalFacture
        );

        // Update the associated 'ventes' record to reflect the new total and paid amount
        await clientDb.query(
            `UPDATE ventes
             SET montant_paye = $1, montant_total = $2, statut_paiement = $3
             WHERE id = $4`,
            [newMontantPayeFacture, finalMontantOriginalFacture, newStatutFacture, invoice.vente_id] // Utilise le nouveau montant total et le nouveau statut
        );


        await clientDb.query('COMMIT');
        res.status(200).json({ message: 'Paiement et/ou montant total de la facture mis à jour avec succès.', invoice: updateInvoiceResult.rows[0] });

    } catch (error) {
        if (clientDb) await clientDb.query('ROLLBACK');
        console.error('Erreur lors de la mise à jour du paiement de la facture:', error);
        res.status(500).json({ error: 'Erreur serveur lors de la mise à jour du paiement de la facture.' });
    } finally {
        if (clientDb) clientDb.release();
    }
});

// PUT /api/factures/:id/cancel - Annuler une facture
router.put('/:id/cancel', async (req, res) => {
    const { id } = req.params;
    const { raison_annulation } = req.body;
    let clientDb;

    if (!raison_annulation) {
        return res.status(400).json({ error: 'La raison de l\'annulation est requise.' });
    }

    try {
        clientDb = await pool.connect();
        await clientDb.query('BEGIN');

        const invoiceResult = await clientDb.query(
            'SELECT vente_id, statut_facture, montant_original_facture, montant_paye_facture FROM factures WHERE id = $1 FOR UPDATE', // Added montant_paye_facture
            [id]
        );
        if (invoiceResult.rows.length === 0) {
            await clientDb.query('ROLLBACK');
            return res.status(404).json({ error: 'Facture non trouvée.' });
        }
        const { vente_id, statut_facture, montant_original_facture, montant_paye_facture } = invoiceResult.rows[0]; // Destructure montant_paye_facture

        if (statut_facture === 'annulee' || statut_facture === 'retour_total') {
            await clientDb.query('ROLLBACK');
            return res.status(400).json({ error: `La facture est déjà ${statut_facture}.` });
        }

        // 1. Mettre à jour le statut de la facture
        const updateInvoiceResult = await clientDb.query(
            `UPDATE factures
             SET statut_facture = 'annulee', date_annulation = NOW(), raison_annulation = $1,
                 montant_paye_facture = 0, montant_actuel_du = 0, montant_rembourse = $2 -- Rembourse le montant payé initialement sur la facture
             WHERE id = $3 RETURNING *`,
            [raison_annulation, montant_paye_facture, id] // Use montant_paye_facture for reimbursement
        );

        // 2. Annuler tous les articles de vente liés à cette facture (via la vente_id)
        const saleItemsResult = await clientDb.query(
            'SELECT id, produit_id, imei, is_special_sale_item FROM vente_items WHERE vente_id = $1 AND statut_vente = \'actif\' FOR UPDATE',
            [vente_id]
        );

        for (const item of saleItemsResult.rows) {
            // Mettre à jour le statut de l'article de vente
            await clientDb.query(
                `UPDATE vente_items SET statut_vente = 'annule', cancellation_reason = $1 WHERE id = $2`,
                [`Annulation facture #${updateInvoiceResult.rows[0].numero_facture}`, item.id]
            );

            // Réactiver le produit dans le stock (toujours, quelle que soit is_special_sale_item)
            if (item.produit_id) { // Ensure produit_id exists
                await clientDb.query(
                    'UPDATE products SET status = $1 WHERE id = $2 AND imei = $3',
                    ['active', item.produit_id, item.imei] // Remettre en 'active' pour qu'il soit visible dans le stock
                );
            }
        }

        // 3. Mettre à jour le statut de la vente associée à 'annulee' et réinitialiser les montants
        await clientDb.query(
            `UPDATE ventes SET statut_paiement = 'annulee', montant_paye = 0, montant_total = 0 WHERE id = $1`, // Reset total and paid for sale
            [vente_id]
        );


        await clientDb.query('COMMIT');
        res.status(200).json({ message: 'Facture et vente associée annulées avec succès.', invoice: updateInvoiceResult.rows[0] });

    } catch (error) {
        if (clientDb) await clientDb.query('ROLLBACK');
        console.error('Erreur lors de l\'annulation de la facture:', error);
        res.status(500).json({ error: 'Erreur serveur lors de l\'annulation de la facture.' });
    } finally {
        if (clientDb) clientDb.release();
    }
});

// POST /api/factures/:id/return-item - Gérer le retour d'un article lié à une facture
router.post('/:id/return-item', async (req, res) => {
    const { id: factureId } = req.params; // ID de la facture
    const { vente_item_id, reason, montant_rembourse_item } = req.body;
    let clientDb;

    if (!vente_item_id || !reason || montant_rembourse_item === undefined || isNaN(parseFloat(montant_rembourse_item)) || parseFloat(montant_rembourse_item) < 0) {
        return res.status(400).json({ error: 'L\'ID de l\'article de vente, la raison et le montant remboursé sont requis.' });
    }

    try {
        clientDb = await pool.connect();
        await clientDb.query('BEGIN');

        // 1. Vérifier la facture et la verrouiller
        const invoiceResult = await clientDb.query(
            'SELECT vente_id, montant_original_facture, montant_actuel_du, montant_paye_facture, montant_rembourse, statut_facture FROM factures WHERE id = $1 FOR UPDATE',
            [factureId]
        );
        if (invoiceResult.rows.length === 0) {
            await clientDb.query('ROLLBACK');
            return res.status(404).json({ error: 'Facture non trouvée.' });
        }
        const invoice = invoiceResult.rows[0];

        if (invoice.statut_facture === 'annulee' || invoice.statut_facture === 'retour_total') {
            await clientDb.query('ROLLBACK');
            return res.status(400).json({ error: `Impossible de traiter un retour pour une facture ${invoice.statut_facture}.` });
        }

        // 2. Récupérer les détails de l'article de vente et le verrouiller
        const saleItemResult = await clientDb.query(
            'SELECT produit_id, imei, is_special_sale_item, prix_unitaire_vente, statut_vente FROM vente_items WHERE id = $1 AND vente_id = $2 FOR UPDATE',
            [vente_item_id, invoice.vente_id]
        );
        if (saleItemResult.rows.length === 0) {
            await clientDb.query('ROLLBACK');
            return res.status(404).json({ error: 'Article de vente non trouvé pour cette facture.' });
        }
        const saleItem = saleItemResult.rows[0];

        if (saleItem.statut_vente === 'annule' || saleItem.statut_vente === 'retourne') {
            await clientDb.query('ROLLBACK');
            return res.status(400).json({ error: `L'article de vente est déjà ${saleItem.statut_vente}.` });
        }

        const parsedMontantRembourseItem = parseFloat(montant_rembourse_item);
        if (parsedMontantRembourseItem > saleItem.prix_unitaire_vente) {
            await clientDb.query('ROLLBACK');
            return res.status(400).json({ error: 'Le montant remboursé ne peut pas être supérieur au prix de vente de l\'article.' });
        }

        // 3. Mettre à jour le statut de l'article de vente à 'retourne'
        await clientDb.query(
            `UPDATE vente_items SET statut_vente = 'retourne', cancellation_reason = $1 WHERE id = $2`,
            [`Retour client (Facture #${invoice.numero_facture}): ${reason}`, vente_item_id]
        );

        // 4. Mettre à jour le statut du produit dans 'products' (toujours, quelle que soit is_special_sale_item)
        if (saleItem.produit_id) { // Ensure produit_id exists
            await clientDb.query(
                `UPDATE products SET status = $1 WHERE id = $2 AND imei = $3`,
                ['active', saleItem.produit_id, saleItem.imei] // Remettre en 'active' si le produit est remis en stock
            );
        }

        // 5. Mettre à jour la facture
        const newMontantRembourseTotal = parseFloat(invoice.montant_rembourse || 0) + parsedMontantRembourseItem;
        const newMontantActuelDu = parseFloat(invoice.montant_actuel_du) - parsedMontantRembourseItem;
        const newMontantPayeFacture = parseFloat(invoice.montant_paye_facture) - parsedMontantRembourseItem; // Reduce paid amount on facture

        let newInvoiceStatus = 'retour_partiel';

        // Check if all items of the sale are now returned or cancelled
        const remainingItemsCheck = await clientDb.query(
            'SELECT COUNT(*) AS total_items, SUM(CASE WHEN statut_vente IN (\'annule\', \'retourne\') THEN 1 ELSE 0 END) AS inactive_items FROM vente_items WHERE vente_id = $1',
            [invoice.vente_id]
        );
        const { total_items, inactive_items } = remainingItemsCheck.rows[0];

        if (parseInt(inactive_items, 10) === parseInt(total_items, 10)) {
            newInvoiceStatus = 'retour_total'; // All items of the sale are returned/cancelled
        } else if (newMontantActuelDu <= 0 && newMontantRembourseTotal > 0) {
             // If all is reimbursed and there were returns, it's a total return
            newInvoiceStatus = 'retour_total';
        }


        const updateInvoiceResult = await clientDb.query(
            `UPDATE factures
             SET montant_actuel_du = $1, montant_rembourse = $2, date_dernier_retour = NOW(), statut_facture = $3, montant_paye_facture = $4
             WHERE id = $5 RETURNING *`,
            [newMontantActuelDu, newMontantRembourseTotal, newInvoiceStatus, newMontantPayeFacture, factureId]
        );

        // Update the associated 'ventes' record
        // Adjust montant_total and montant_paye in 'ventes' based on the return
        const currentSaleTotalResult = await clientDb.query(
            'SELECT SUM(prix_unitaire_vente * quantite_vendue) AS current_total_sale_value FROM vente_items WHERE vente_id = $1 AND statut_vente = \'actif\'',
            [invoice.vente_id]
        );
        const currentTotalSaleValue = parseFloat(currentSaleTotalResult.rows[0].current_total_sale_value || 0);

        let newSaleStatus = 'paiement_partiel';
        if (newMontantPayeFacture >= currentTotalSaleValue) { // Compare new paid amount with remaining total sale value
             newSaleStatus = 'payee_integralement';
        } else if (newMontantPayeFacture === 0 && currentTotalSaleValue > 0) {
            newSaleStatus = 'en_attente_paiement';
        } else if (currentTotalSaleValue === 0) { // If all items are returned/cancelled, sale total becomes 0
            newSaleStatus = 'annulee';
        }


        await clientDb.query(
            `UPDATE ventes SET
                montant_total = $1, -- Montant total de la vente ajusté
                montant_paye = $2, -- Montant payé de la vente ajusté
                statut_paiement = $3 -- Statut de paiement de la vente ajusté
             WHERE id = $4`,
            [currentTotalSaleValue, newMontantPayeFacture, newSaleStatus, invoice.vente_id]
        );

        await clientDb.query('COMMIT');
        res.status(200).json({ message: 'Retour d\'article traité avec succès et facture mise à jour.', invoice: updateInvoiceResult.rows[0] });

    } catch (error) {
        if (clientDb) await clientDb.query('ROLLBACK');
        console.error('Erreur lors du traitement du retour d\'article de facture:', error);
        res.status(500).json({ error: 'Erreur serveur lors du traitement du retour d\'article de facture.' });
    } finally {
        if (clientDb) clientDb.release();
    }
});

// POST /api/factures/:id/print - Générer un PDF de la facture (Placeholder)
router.post('/:id/print', async (req, res) => {
    const { id } = req.params;
    // Ici, vous intégreriez la logique pour générer un PDF.
    // Cela nécessiterait une bibliothèque comme 'puppeteer' ou 'html-pdf'.
    // Pour l'instant, c'est un placeholder.
    res.status(200).json({ message: `Génération du PDF pour la facture ${id} (fonctionnalité à implémenter).` });
});

module.exports = router;
