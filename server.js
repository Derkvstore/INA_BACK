// backend/server.js
const express = require('express');
const cors = require('cors');
const dotenv = require('dotenv');
dotenv.config();

const { pool } = require('./db');
const { registerUser, loginUser } = require('./auth');

const clientsRoutes = require('./clients');
const productRoutes = require('./products');
const ventesRoutes = require('./ventes');
const reportsRouter = require('./reports');
const returnsRouter = require('./returns');
const remplacerRouter = require('./remplacements');
const fournisseursRoutes = require('./fournisseurs');
const facturesRoutes = require('./factures');
const specialOrdersRoutes = require('./specialOrders');

const app = express();
const PORT = process.env.PORT || 3001;

// ✅ CORS autorisé pour Railway Front + localhost et ton site Vercel
const allowedOrigins = [
   'https://inaback-production.up.railway.app',
//    'https://daff-telecom.vercel.app', // ✅ CORRECT sans /
   'http://localhost:5173'
];


// ✅ Middleware CORS propre
app.use(cors({
  origin: function (origin, callback) {
    if (!origin || allowedOrigins.includes(origin)) {
      callback(null, true);
    } else {
      callback(new Error('Not allowed by CORS'));
    }
  },
  credentials: true,
  methods: ['GET', 'POST', 'PUT', 'DELETE', 'OPTIONS'],
  allowedHeaders: ['Content-Type', 'Authorization'],
}));

// ✅ Parser JSON
app.use(express.json());

/* --- ROUTES --- */
// Pour l'authentification, utilisez les fonctions directement avec app.post()
app.post('/api/login', loginUser);
app.post('/api/register', registerUser); // Si vous avez une route d'enregistrement

// Utilisez app.use() pour les autres routeurs qui exportent "router"
// Vérifiez que les chemins d'accès ici correspondent à la structure de vos fichiers
app.use('/api/clients', clientsRoutes);
app.use('/api/products', productRoutes);
app.use('/api/ventes', ventesRoutes);
app.use('/api/reports', reportsRouter);
app.use('/api/returns', returnsRouter);
app.use('/api/remplacements', remplacerRouter);
app.use('/api/fournisseurs', fournisseursRoutes);
app.use('/api/factures', facturesRoutes);
app.use('/api/special-orders', specialOrdersRoutes); // NOUVELLE ROUTE pour les commandes spéciales

// Nouvelle route GET pour calculer les bénéfices totaux et détaillés
app.get('/api/benefices', async (req, res) => {
    try {
        let query = `
            SELECT
                vi.id AS vente_item_id,
                vi.marque,
                vi.modele,
                vi.stockage,
                vi.type,
                vi.type_carton,
                vi.imei,
                vi.prix_unitaire_achat,
                vi.prix_unitaire_vente,
                vi.quantite_vendue,
                (vi.prix_unitaire_vente - vi.prix_unitaire_achat) AS benefice_unitaire_produit,
                (vi.quantite_vendue * (vi.prix_unitaire_vente - vi.prix_unitaire_achat)) AS benefice_total_par_ligne,
                v.date_vente -- SÉLECTION DE LA DATE DE VENTE
            FROM
                vente_items vi
            JOIN
                ventes v ON vi.vente_id = v.id
            WHERE
                vi.statut_vente = 'actif' -- Ne considère que les articles activement vendus
                AND v.statut_paiement = 'payee_integralement' -- La vente doit être intégralement payée (inclut détail et gros)
        `;
        const queryParams = [];
        let paramIndex = 1;

        const { date } = req.query; // Récupère le paramètre 'date' de la requête (ex: /api/benefices?date=2023-01-15)

        if (date) {
            // Validation simple du format de la date (YYYY-MM-DD)
            if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
                return res.status(400).json({ error: 'Format de date invalide. Utilisez YYYY-MM-DD.' });
            }
            // Condition pour filtrer par la date de vente
            query += ` AND DATE(v.date_vente) = $${paramIndex}`;
            queryParams.push(date);
            paramIndex++;
        }

        query += ` ORDER BY v.date_vente DESC;`; // Trie par date de vente pour voir les plus récents en premier

        const itemsResult = await pool.query(query, queryParams);

        const soldItems = itemsResult.rows;

        // --- DÉBOGAGE : AFFICHER LES DONNÉES AVANT L'ENVOI AU FRONTEND ---
        console.log("Données des articles vendus envoyées au frontend (vérifiez 'date_vente'):");
        soldItems.forEach(item => {
            console.log(`  IMEI: ${item.imei}, Date Vente: ${item.date_vente}, Bénéfice: ${item.benefice_total_par_ligne}`);
        });
        // --- FIN DÉBOGAGE ---

        // Calcul du bénéfice total global à partir des résultats détaillés
        let totalBeneficeGlobal = 0;
        soldItems.forEach(item => {
            totalBeneficeGlobal += parseFloat(item.benefice_total_par_ligne);
        });

        // Envoie la liste des articles vendus avec leurs bénéfices et le bénéfice total global
        res.json({
            sold_items: soldItems,
            total_benefice_global: parseFloat(totalBeneficeGlobal)
        });

    } catch (err) {
        console.error('Erreur lors du calcul des bénéfices:', err);
        res.status(500).json({ error: 'Erreur interne du serveur lors du calcul des bénéfices.' });
    }
});


/* --- DÉMARRAGE DU SERVEUR --- */
app.listen(PORT, () => {
  console.log('✅ Connexion à la base de données réussie');
  console.log(`🚀 Serveur backend lancé sur le port ${PORT}`);
});
