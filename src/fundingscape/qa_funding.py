"""Link quantum applications to fundingscape grants for TAM computation.

Queries the fundingscape grant_award_deduped view with application-specific
keyword patterns and stores results in the quantum_applications funding_link table.
"""

from __future__ import annotations

from datetime import UTC, datetime

import duckdb

from fundingscape import DB_PATH, QA_DB_PATH
from fundingscape.qa_db import get_connection as get_qa_connection
from fundingscape.qa_db import upsert_funding_link
from fundingscape.qa_models import FundingLink

# ---------------------------------------------------------------------------
# Keyword patterns per application name.
#
# Each entry maps application.name -> list of ILIKE patterns.
# A grant matches if (project_title OR abstract) matches ANY pattern.
# Patterns are joined with OR, so keep them precise to avoid false positives.
# ---------------------------------------------------------------------------

APPLICATION_KEYWORDS: dict[str, list[str]] = {
    # -- Cryptography --
    "Integer factorisation": [
        "%integer factor%",
        "%prime factor%",
        "%shor's algorithm%",
        "%shor algorithm%",
        "%RSA%quantum comput%",
        "%quantum%cryptanalysis%",
        "%post-quantum%cryptograph%",
    ],
    "Discrete logarithm over finite fields": [
        "%discrete logarithm%",
        "%diffie%hellman%quantum%",
        "%shor%logarithm%",
    ],
    "Elliptic curve discrete logarithm": [
        "%elliptic curve%quantum%",
        "%ECDSA%quantum%",
        "%ECDH%quantum%",
        "%ECC%quantum%attack%",
    ],
    "Symmetric key search": [
        "%grover%search%",
        "%grover%algorithm%",
        "%quantum%brute%force%",
        "%quantum%key search%",
    ],
    "Hash collision finding": [
        "%hash%collision%quantum%",
        "%quantum%hash%",
        "%BHT%algorithm%",
    ],

    # -- Chemistry --
    "Molecular ground state energy estimation": [
        "%molecular%ground state%quantum%",
        "%quantum comput%chemistry%",
        "%variational quantum eigensolver%",
        "%VQE%",
        "%quantum phase estimation%molecular%",
        "%electronic structure%quantum comput%",
        "%quantum%chemical%simulation%quantum%",
        "%quantum algorithm%chemistry%",
    ],
    "Molecular excited state computation": [
        "%excited state%quantum comput%",
        "%excited state%quantum simulat%",
        "%quantum%photochemistry%comput%",
        "%qEOM%",
        "%SSVQE%",
        "%excited state%VQE%",
        "%excited state%variational%quantum%",
    ],
    "Molecular dynamics on quantum PES": [
        "%molecular dynamics%quantum comput%",
        "%quantum%potential energy surface%quantum comput%",
        "%ab initio%quantum comput%",
        "%born%oppenheimer%quantum comput%",
        "%molecular dynamics%quantum algorithm%",
    ],
    "Binding affinity prediction": [
        "%binding affinity%quantum%",
        "%drug%binding%quantum%",
        "%protein%ligand%quantum%",
        "%quantum%drug%design%",
    ],
    "Retrosynthesis planning": [
        "%retrosynthesis%quantum%",
        "%synthetic route%quantum%",
        "%quantum%synthesis%planning%",
    ],
    "Catalytic reaction mechanism elucidation": [
        "%catalys%quantum%comput%",
        "%catalys%quantum%simulat%",
        "%reaction mechanism%quantum%",
        "%transition state%quantum comput%",
        "%nitrogen fixation%quantum%",
        "%CO2 reduction%quantum%",
    ],
    "Vibrational structure and molecular spectra": [
        "%vibrational%quantum comput%",
        "%vibrational%quantum simulat%",
        "%molecular spectra%quantum comput%",
        "%anharmonic%quantum comput%",
        "%vibronic%quantum comput%",
    ],

    # -- Materials Science --
    "High-Tc superconductor simulation": [
        "%superconductor%simulat%",
        "%high%temperature%superconducti%quantum%",
        "%hubbard%model%quantum%",
        "%cuprate%quantum%",
        "%quantum%superconducti%simulat%",
    ],
    "Battery electrolyte and electrode simulation": [
        "%battery%quantum%comput%",
        "%battery%quantum%simulat%",
        "%electrolyte%quantum%",
        "%lithium%quantum%comput%",
        "%electrode%quantum%simulat%",
    ],
    "Topological phase classification": [
        "%topological%phase%quantum%",
        "%topological%insulator%quantum%simulat%",
        "%topological%invariant%quantum%comput%",
        "%topological%quantum%matter%",
    ],
    "FeMo-cofactor (nitrogenase) electronic structure": [
        "%nitrogenase%quantum%",
        "%FeMo%cofactor%",
        "%FeMoco%quantum%",
        "%nitrogen%fixation%quantum%comput%",
    ],

    # -- Quantum Simulation --
    "Time evolution of quantum spin systems": [
        "%hamiltonian simulation%",
        "%quantum%spin%simulat%",
        "%trotter%suzuki%",
        "%quantum%spin%chain%",
        "%quantum%spin%model%simulat%",
        "%product formula%quantum%",
    ],
    "Quantum field theory simulation": [
        "%lattice gauge%quantum%",
        "%quantum%field theory%simulat%",
        "%quantum%chromodynamics%simulat%",
        "%QCD%quantum comput%",
        "%QED%quantum simulat%",
        "%lattice%gauge%qubit%",
    ],
    "Ground state preparation of local Hamiltonians": [
        "%ground state preparation%quantum%",
        "%variational%quantum%eigensolver%",
        "%adiabatic%state%preparation%quantum%",
        "%quantum comput%ground state%",
    ],
    "Lindbladian dynamics simulation": [
        "%lindblad%quantum%",
        "%open quantum system%simulat%",
        "%dissipative%quantum%simulat%",
        "%quantum%noise%simulat%",
        "%quantum%decoherence%simulat%",
    ],

    # -- Optimisation --
    "Max-Cut and graph partitioning": [
        "%max%cut%quantum%",
        "%QAOA%",
        "%graph partition%quantum%",
        "%quantum%approximate%optimization%",
    ],
    "Travelling salesman and vehicle routing": [
        "%travelling salesman%",
        "%traveling salesman%",
        "%vehicle routing%quantum%",
        "%quantum%routing%optim%",
    ],
    "Boolean satisfiability (SAT)": [
        "%satisfiability%quantum%",
        "%boolean satisf%",
        "%boolean%quantum%",
        "%quantum% SAT %",
        "%MAX-SAT%quantum%",
    ],
    "Convex optimisation / SDP solving": [
        "%semidefinite%quantum%",
        "%SDP%quantum%",
        "%convex%optim%quantum%",
        "%interior point%quantum%",
    ],
    "Unstructured database search": [
        "%grover%search%",
        "%grover%algorithm%",
        "%unstructured%search%quantum%",
        "%amplitude%amplification%",
    ],
    "Job-shop scheduling": [
        "%scheduling%quantum%",
        "%job%shop%quantum%",
        "%quantum%scheduling%",
    ],
    "Quadratic unconstrained binary optimisation (QUBO)": [
        "%QUBO%",
        "%quadratic unconstrained%",
        "%quantum%annealing%optim%",
        "%ising formulation%quantum%",
    ],

    # -- Finance --
    "Monte Carlo option pricing": [
        "%option%pricing%quantum%",
        "%quantum%monte carlo%financ%",
        "%quantum%amplitude estimation%financ%",
        "%derivative%pricing%quantum%",
        "%quantum%financ%",
    ],
    "Value-at-Risk and CVaR estimation": [
        "%value%at%risk%quantum%",
        "%CVaR%quantum%",
        "%risk%estimation%quantum%",
        "%quantum%risk%analysis%",
    ],
    "Portfolio optimisation": [
        "%portfolio%optim%quantum%",
        "%portfolio%quantum%",
        "%asset%allocation%quantum%",
        "%markowitz%quantum%",
    ],
    "Credit scoring and fraud detection": [
        "%fraud%detection%quantum%",
        "%credit%scoring%quantum%",
        "%anomaly%detection%quantum%",
    ],
    "Exotic derivative pricing (path-dependent options)": [
        "%exotic%derivative%quantum%",
        "%path%dependent%quantum%",
        "%asian%option%quantum%",
        "%barrier%option%quantum%",
    ],

    # -- Machine Learning --
    "Quantum kernel methods / QSVM": [
        "%quantum kernel%method%",
        "%quantum kernel%estimat%",
        "%QSVM%",
        "%quantum%support vector%",
        "%quantum%feature map%",
        "%quantum%classifier%",
    ],
    "Quantum generative adversarial networks": [
        "%quantum GAN%",
        "%quantum%generative%adversarial%",
        "%born machine%",
        "%quantum%generative model%",
        "%QGAN%",
    ],
    "Quantum Boltzmann sampling": [
        "%quantum%boltzmann%",
        "%quantum%gibbs%sampling%",
        "%quantum%metropolis%",
    ],
    "Quantum principal component analysis": [
        "%quantum%PCA%",
        "%quantum%principal component%",
    ],
    "Quantum recommendation systems": [
        "%quantum%recommendation%",
        "%quantum%collaborative filter%",
        "%dequanti%",
    ],
    "Quantum-enhanced reinforcement learning": [
        "%quantum%reinforcement%learning%",
        "%quantum reinforcement learn%",
        "%quantum% RL %",
    ],

    # -- Linear Algebra --
    "Solving sparse linear systems (HHL)": [
        "%HHL algorithm%",
        "%quantum%linear system%",
        "%quantum%linear solver%",
        "%harrow%hassidim%lloyd%",
    ],
    "Quantum ODE/PDE solvers": [
        "%quantum%differential equation%",
        "%quantum PDE%",
        "%quantum ODE %",
        "%quantum% ODE solver%",
        "%carleman%quantum%",
        "%quantum%partial differential%",
        "%quantum algorithm%differential%",
    ],
    "Quantum phase estimation": [
        "%phase estimation%",
        "%QPE%",
        "%eigenvalue%quantum%",
    ],
    "Quantum singular value transformation": [
        "%singular value%quantum%",
        "%QSVT%",
        "%quantum signal processing%",
        "%QSP%qubit%",
    ],

    # -- Logistics --
    "Supply chain network optimisation": [
        "%supply chain%quantum%",
        "%logistics%quantum%",
        "%warehouse%quantum%optim%",
    ],
    "Traffic flow optimisation": [
        "%traffic%quantum%",
        "%traffic%flow%quantum%",
        "%routing%quantum%optim%",
    ],

    # -- Energy --
    "Power grid unit commitment": [
        "%power grid%quantum%",
        "%unit commitment%quantum%",
        "%energy%grid%quantum%",
        "%power%dispatch%quantum%",
    ],
    "Nuclear structure calculation": [
        "%nuclear%structure%quantum comput%",
        "%nuclear%quantum simulat%",
        "%nuclear%binding%quantum%",
        "%atomic nucle%quantum%",
    ],

    # -- Mathematics --
    "Period finding and hidden subgroup problem": [
        "%hidden subgroup%",
        "%period finding%quantum%",
        "%quantum%fourier%transform%",
    ],
    "Quantum algorithm for Jones polynomial": [
        "%jones polynomial%",
        "%knot%invariant%quantum%",
        "%topological%quantum comput%",
    ],
    "Topological data analysis (Betti numbers)": [
        "%betti number%quantum%",
        "%persistent homology%quantum%",
        "%topological%data%analysis%quantum%",
    ],
    "Non-abelian hidden subgroup problem": [
        "%non%abelian%hidden%subgroup%",
        "%graph isomorphism%quantum%",
        "%dihedral%hidden%subgroup%",
    ],

    # -- Computer Science --
    "Boson sampling": [
        "%boson sampling%",
        "%gaussian%boson%sampling%",
        "%linear optical%quantum%",
    ],
    "Random circuit sampling": [
        "%random circuit%sampling%",
        "%quantum%supremacy%",
        "%quantum%advantage%demonstrat%",
    ],
    "Element distinctness and graph property testing": [
        "%element distinctness%",
        "%quantum walk%graph%",
        "%quantum walk%search%",
    ],
    "Quantum Monte Carlo integration": [
        "%quantum%amplitude%estimation%",
        "%quantum%monte carlo%advantage%",
        "%quantum%numerical integration%",
        "%quantum%quadratic speedup%monte%",
        "%amplitude estimation%quantum%",
    ],

    # -- Life Sciences --
    "Molecular docking and virtual screening": [
        "%molecular docking%quantum%",
        "%virtual screening%quantum%",
        "%drug%screen%quantum%",
    ],
    "Sequence alignment and genomic analysis": [
        "%sequence alignment%quantum%",
        "%genom%quantum%comput%",
        "%DNA%quantum%comput%",
        "%bioinformatics%quantum%",
    ],
    "Protein folding energy landscape exploration": [
        "%protein%folding%quantum%",
        "%protein%structure%quantum%comput%",
        "%protein%conformation%quantum%",
    ],

    # -- Engineering --
    "CFD via quantum linear algebra": [
        "%fluid dynamics%quantum%",
        "%navier%stokes%quantum%",
        "%lattice boltzmann%quantum%",
        "%CFD%quantum%",
    ],

    # -- Earth Science --
    "Climate and weather simulation": [
        "%climate%quantum%comput%",
        "%weather%quantum%comput%",
        "%climate%model%quantum%",
        "%atmospheric%quantum%simulat%",
    ],

    # -- Telecommunications --
    "Wireless network resource allocation": [
        "%wireless%quantum%optim%",
        "%spectrum%allocation%quantum%",
        "%network%resource%quantum%",
        "%5G%quantum%",
        "%6G%quantum%",
    ],

    # -- Tranche 2: Optimisation (expanded) --
    "Graph colouring": [
        "%graph colo%quantum%",
        "%graph colo%QAOA%",
        "%chromatic%quantum%",
    ],
    "Bin packing and knapsack": [
        "%bin packing%quantum%",
        "%knapsack%quantum%",
        "%quantum%bin pack%",
    ],
    "Maximum independent set": [
        "%maximum independent set%",
        "%independent set%rydberg%",
        "%independent set%QAOA%",
        "%independent set%quantum anneal%",
        "%independent set%quantum optim%",
    ],
    "Set cover and minimum vertex cover": [
        "%set cover%quantum%",
        "%vertex cover%quantum%",
    ],
    "Maximum flow and minimum cut": [
        "%maximum flow%quantum%",
        "%minimum cut%quantum%",
        "%network flow%quantum%",
        "%max-flow%quantum%",
    ],
    "Gradient estimation and optimisation": [
        "%quantum%gradient%estimation%",
        "%jordan%gradient%quantum%",
        "%quantum%gradient%descent%",
    ],

    # -- Tranche 2: Cryptography (expanded) --
    "Lattice problem solving (LWE/SVP)": [
        "%lattice%quantum%crypto%",
        "%LWE%quantum%",
        "%shortest vector%quantum%",
        "%post-quantum%lattice%",
        "%lattice%sieving%quantum%",
    ],
    "Code-based cryptanalysis": [
        "%code%based%cryptograph%",
        "%McEliece%quantum%",
        "%information set decoding%quantum%",
    ],
    "Quantum random number generation": [
        "%quantum random number%",
        "%QRNG%",
        "%quantum%random%generat%",
        "%certifiable random%quantum%",
    ],

    # -- Tranche 2: Quantum Computing (new domain) --
    "Quantum error correction decoding": [
        "%quantum error correct%",
        "%surface code%",
        "%colour code%quantum%",
        "%color code%quantum%",
        "%fault%tolerant%quantum%",
        "%quantum%LDPC%",
        "%topological code%",
    ],
    "Quantum circuit optimisation and compilation": [
        "%quantum circuit%optim%",
        "%quantum%compil%",
        "%gate synthesis%",
        "%ZX%calculus%",
        "%solovay%kitaev%",
    ],
    "Quantum volume and fidelity benchmarking": [
        "%quantum volume%",
        "%quantum%benchmarking%",
        "%randomised benchmarking%",
        "%randomized benchmarking%",
        "%quantum%fidelity%characteris%",
    ],

    # -- Tranche 2: Computer Science (expanded) --
    "Quantum walk on graphs": [
        "%quantum walk%",
        "%quantum%random walk%",
    ],
    "Quantum interactive proofs (QIP = PSPACE)": [
        "%quantum interactive proof%",
        "%QIP%PSPACE%",
        "%quantum%verif%protocol%",
    ],
    "Quantum query speedup for formula evaluation": [
        "%formula evaluation%quantum%",
        "%span program%quantum%",
        "%AND-OR%tree%quantum%",
        "%quantum%query%complexity%",
    ],

    # -- Tranche 2: Quantum Simulation (expanded) --
    "Quantum magnetism simulation": [
        "%quantum%magnet%simulat%",
        "%spin liquid%quantum%",
        "%frustrated magnet%quantum%",
        "%quantum%spin liquid%",
    ],
    "Parton shower and scattering simulation": [
        "%parton%quantum%",
        "%scattering%quantum%simulat%",
        "%particle%collision%quantum%",
        "%high energy physics%quantum comput%",
    ],
    "Quantum gravity and cosmology simulation": [
        "%quantum gravity%simulat%",
        "%holograph%quantum%simulat%",
        "%AdS/CFT%quantum%comput%",
        "%loop quantum gravity%simulat%",
        "%quantum%cosmolog%simulat%",
    ],
    "Nuclear scattering cross-section computation": [
        "%nuclear%scattering%quantum%",
        "%nuclear%reaction%quantum comput%",
        "%nuclear%cross section%quantum%",
    ],

    # -- Tranche 2: Machine Learning (expanded) --
    "Quantum clustering (k-means, spectral)": [
        "%quantum%clustering%",
        "%quantum%k-means%",
        "%quantum%spectral cluster%",
    ],
    "Quantum neural networks / variational classifiers": [
        "%quantum neural network%",
        "%variational%classifier%quantum%",
        "%parameterised quantum circuit%",
        "%parameterized quantum circuit%",
        "%quantum%barren plateau%",
    ],
    "Quantum natural language processing": [
        "%quantum%natural language%",
        "%QNLP%",
        "%quantum%linguist%",
        "%DisCoCat%quantum%",
        "%lambeq%",
    ],
    "Quantum anomaly detection": [
        "%quantum%anomaly detect%",
        "%quantum%autoencoder%",
        "%quantum%outlier%",
    ],

    # -- Tranche 2: Finance (expanded) --
    "Quantum-enhanced market making and execution": [
        "%market making%quantum%",
        "%trade execution%quantum%",
        "%quantum%trading%",
    ],
    "Financial clearing and netting optimisation": [
        "%clearing%quantum%",
        "%netting%quantum%",
        "%settlement%quantum%",
    ],

    # -- Tranche 2: Materials & Chemistry (expanded) --
    "Photovoltaic material simulation": [
        "%photovoltaic%quantum%simulat%",
        "%solar cell%quantum%simulat%",
        "%perovskite%quantum comput%",
        "%exciton%quantum comput%",
    ],
    "Photosynthetic energy transfer simulation": [
        "%photosynthes%quantum%",
        "%light%harvest%quantum%",
        "%FMO%quantum%",
    ],
    "Electrochemical reaction simulation": [
        "%electrochemical%quantum comput%",
        "%electrocatal%quantum%",
        "%redox%quantum comput%",
    ],

    # -- Tranche 2: Logistics (expanded) --
    "Flight gate assignment and airline scheduling": [
        "%flight%gate%quantum%",
        "%airline%schedule%quantum%",
        "%aircraft%assign%quantum%",
    ],
    "Warehouse picking and layout optimisation": [
        "%warehouse%quantum%",
        "%picking%optim%quantum%",
    ],

    # -- Tranche 2: Energy (expanded) --
    "Plasma confinement simulation": [
        "%plasma%quantum comput%",
        "%fusion%quantum comput%",
        "%magnetohydrodynamic%quantum comput%",
        "%tokamak%quantum comput%",
        "%plasma%confinement%quantum%",
    ],
    "Renewable energy integration and storage scheduling": [
        "%renewable%quantum%optim%",
        "%battery%storage%quantum%optim%",
        "%energy storage%quantum%",
        "%demand response%quantum%",
    ],

    # -- Tranche 2: Life Sciences (expanded) --
    "Phylogenetic tree reconstruction": [
        "%phylogenet%quantum%",
        "%evolution%tree%quantum%",
    ],
    "Medical image classification and segmentation": [
        "%medical%imag%quantum%",
        "%quantum%radiology%",
        "%quantum%histopathol%",
        "%quantum%MRI%classif%",
    ],

    # -- Tranche 3: Condensed Matter & Many-Body --
    "Bose-Hubbard model simulation": [
        "%bose%hubbard%",
        "%bosonic%lattice%quantum%simulat%",
        "%superfluid%mott%quantum%",
    ],
    "Phonon and electron-phonon coupling simulation": [
        "%electron%phonon%quantum comput%",
        "%electron%phonon%quantum simulat%",
        "%polaron%quantum comput%",
    ],
    "Quantum Hall effect simulation": [
        "%quantum hall%simulat%",
        "%fractional quantum hall%",
        "%anyon%quantum comput%",
        "%anyon%quantum simulat%",
    ],
    "Thermalisation and many-body localisation": [
        "%many-body localis%",
        "%many-body localiz%",
        "%thermalis%quantum%simulat%",
        "%thermaliz%quantum%simulat%",
        "%many body locali%quantum%",
    ],
    "Quantum quench dynamics": [
        "%quantum quench%",
        "%quench dynamics%quantum%",
        "%non-equilibrium%quantum%simulat%",
    ],

    # -- Tranche 3: Quantum Computing Infrastructure --
    "Quantum error mitigation": [
        "%error mitigation%",
        "%zero%noise%extrapolation%",
        "%probabilistic error cancel%",
        "%quantum%noise%mitigation%",
    ],
    "Quantum state tomography and verification": [
        "%quantum%tomography%",
        "%classical shadow%",
        "%quantum state%verification%",
        "%fidelity estimation%",
    ],
    "Quantum network routing and entanglement distribution": [
        "%quantum network%",
        "%quantum internet%",
        "%entanglement distribution%",
        "%quantum repeater%",
    ],

    # -- Tranche 3: Optimisation (more niche) --
    "Minimum spanning tree and Steiner tree": [
        "%spanning tree%quantum%",
        "%steiner tree%quantum%",
    ],
    "Facility location and p-median": [
        "%facility location%quantum%",
        "%p-median%quantum%",
    ],
    "Stochastic optimisation under uncertainty": [
        "%stochastic optim%quantum%",
        "%robust optim%quantum%",
        "%optim%uncertainty%quantum%",
    ],

    # -- Tranche 3: Materials Science (gaps) --
    "Semiconductor defect simulation": [
        "%defect%quantum comput%",
        "%NV cent%quantum comput%",
        "%vacancy%quantum simulat%",
        "%point defect%quantum%",
    ],
    "Polymer and soft matter simulation": [
        "%polymer%quantum comput%",
        "%soft matter%quantum%",
        "%polymer%quantum simulat%",
    ],
    "Permanent magnet and rare-earth material simulation": [
        "%rare%earth%quantum comput%",
        "%permanent magnet%quantum%",
        "%magnetic anisotropy%quantum comput%",
    ],

    # -- Tranche 3: Chemistry (gaps) --
    "Open-shell and radical species computation": [
        "%open%shell%quantum comput%",
        "%radical%quantum comput%",
        "%biradical%quantum%",
        "%multireference%quantum comput%",
    ],
    "Atmospheric reaction rate computation": [
        "%atmospheric%quantum comput%",
        "%photodissociation%quantum%",
        "%atmospheric%reaction%quantum%",
    ],

    # -- Tranche 3: Machine Learning (gaps) --
    "Quantum transfer learning": [
        "%quantum%transfer learn%",
        "%hybrid%classical%quantum%neural%",
    ],
    "Quantum data loading and state preparation": [
        "%state preparation%quantum%",
        "%QRAM%",
        "%quantum%data%loading%",
        "%amplitude encoding%",
    ],

    # -- Tranche 3: Mathematics (gaps) --
    "Approximate counting and partition functions": [
        "%approximate counting%quantum%",
        "%partition function%quantum comput%",
        "%quantum%counting%algorithm%",
    ],
    "Pell equation and principal ideal problem": [
        "%pell%equation%quantum%",
        "%principal ideal%quantum%",
        "%unit group%quantum%",
    ],

    # -- Tranche 3: Engineering (expanded) --
    "Finite element analysis via quantum linear solvers": [
        "%finite element%quantum%",
        "%structural analysis%quantum comput%",
        "%structural%mechanics%quantum%",
    ],
    "Quantum signal processing and filtering": [
        "%quantum signal processing%",
        "%QSP%qubit%",
    ],

    # -- Tranche 3: Earth Science --
    "Seismic wave inversion": [
        "%seismic%quantum%",
        "%waveform inversion%quantum%",
        "%subsurface%quantum%",
    ],

    # -- Tranche 3: Agriculture --
    "Fertiliser catalyst design (Haber-Bosch alternative)": [
        "%haber%bosch%quantum%",
        "%nitrogen fixation%quantum comput%",
        "%ammonia%catalyst%quantum%",
        "%fertiliser%quantum%",
        "%fertilizer%quantum%",
    ],

    # -- Tranche 3: Engineering / Automotive --
    "Manufacturing process optimisation": [
        "%manufacturing%quantum%optim%",
        "%manufacturing%quantum%comput%",
        "%production%optim%quantum%",
    ],
    "Crash simulation and structural optimisation": [
        "%crash%simulation%quantum%",
        "%structural%optim%quantum%",
        "%topology%optim%quantum%",
    ],

    # -- Tranche 3: Defence --
    "Quantum radar and target detection": [
        "%quantum radar%",
        "%quantum illumination%",
        "%quantum%target%detection%",
    ],
    "Quantum-enhanced GPS-denied navigation": [
        "%quantum%navigation%",
        "%quantum%inertial%",
        "%atom%interferom%navigation%",
    ],

    # -- Tranche 3: Quantum-Inspired --
    "Quantum-inspired classical algorithms": [
        "%quantum%inspired%algorithm%",
        "%dequantis%",
        "%dequantiz%",
        "%quantum%inspired%classical%",
    ],

    # -- Tranche 3: Information Theory --
    "Quantum channel capacity computation": [
        "%quantum channel%capacity%",
        "%quantum%capacity%computation%",
        "%quantum%communication%capacity%",
    ],
}


def _build_where_clause(patterns: list[str]) -> str:
    """Build a SQL WHERE clause from ILIKE patterns.

    Matches against both project_title and abstract.
    Escapes single quotes in patterns.
    """
    conditions = []
    for p in patterns:
        safe = p.replace("'", "''")
        conditions.append(f"project_title ILIKE '{safe}'")
        conditions.append(f"abstract ILIKE '{safe}'")
    return " OR ".join(conditions)


def compute_funding_links(
    fundingscape_path: str | None = None,
    qa_path: str | None = None,
    verbose: bool = False,
) -> dict[str, dict]:
    """Query fundingscape for each application and store TAM results.

    Returns a dict of {app_name: {grant_count, total_funding_eur, top_funders}}.
    """
    fs_path = fundingscape_path or DB_PATH
    fs_conn = duckdb.connect(fs_path, read_only=True)
    qa_conn = get_qa_connection(qa_path)

    # Get all applications
    apps = qa_conn.execute(
        "SELECT id, name FROM application ORDER BY name"
    ).fetchall()

    now = datetime.now(UTC).isoformat()
    results = {}

    for app_id, app_name in apps:
        patterns = APPLICATION_KEYWORDS.get(app_name)
        if not patterns:
            if verbose:
                print(f"  SKIP {app_name} (no keywords defined)")
            continue

        where = _build_where_clause(patterns)
        query_pattern = " OR ".join(patterns)

        # Count and sum funding
        row = fs_conn.execute(f"""
            SELECT
                COUNT(*) as cnt,
                COALESCE(SUM(COALESCE(total_funding, total_funding_estimated, 0)), 0) as funding
            FROM grant_award_deduped
            WHERE {where}
        """).fetchone()

        grant_count = row[0]
        total_funding = row[1]

        # Top funders (by grant count)
        top_funders_rows = fs_conn.execute(f"""
            SELECT f.short_name, COUNT(*) as cnt
            FROM grant_award_deduped g
            LEFT JOIN funder f ON g.funder_id = f.id
            WHERE {where}
            GROUP BY f.short_name
            ORDER BY cnt DESC
            LIMIT 5
        """).fetchall()
        top_funders = ", ".join(
            f"{r[0] or 'Unknown'}({r[1]})" for r in top_funders_rows
        )

        # Store in funding_link
        link = FundingLink(
            application_id=app_id,
            query_pattern=query_pattern,
            grant_count=grant_count,
            total_funding_eur=total_funding,
            top_funders=top_funders,
            last_computed=now,
        )
        upsert_funding_link(qa_conn, link)

        results[app_name] = {
            "grant_count": grant_count,
            "total_funding_eur": total_funding,
            "top_funders": top_funders,
        }

        if verbose:
            funding_m = total_funding / 1e6 if total_funding else 0
            print(
                f"  {app_name:55s}  {grant_count:>6,} grants  "
                f"{funding_m:>10,.1f}M EUR  [{top_funders}]"
            )

    fs_conn.close()
    qa_conn.close()
    return results
