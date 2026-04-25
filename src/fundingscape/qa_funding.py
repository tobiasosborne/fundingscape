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
        # NB: bare "integer factor" matches lots of pure-math papers; require quantum context
        "%integer factor%quantum%",
        "%quantum%integer factor%",
        "%prime factor%quantum%",
        "%quantum%prime factor%",
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
        # Tightened: bare "superconductor simulat" matches classical BdG / DFT papers;
        # require high-Tc or quantum-computing context
        "%high-Tc%superconducti%",
        "%high temperature%superconducti%simulat%",
        "%high temperature%superconducti%quantum%",
        "%hubbard%model%simulat%",
        "%cuprate%simulat%",
        "%cuprate%quantum%",
        "%quantum%superconducti%simulat%",
        "%doped%hubbard%",
        "%t-J model%",
    ],
    "Battery electrolyte and electrode simulation": [
        "%battery%quantum%comput%",
        "%battery%quantum%simulat%",
        "%electrolyte%quantum%",
        "%lithium%quantum%comput%",
        "%electrode%quantum%simulat%",
    ],
    "Topological phase classification": [
        # Tightened: require explicit classification/computation context, not just any "topological X quantum"
        "%topological%phase%classification%",
        "%topological%phase%diagram%quantum%",
        "%topological%insulator%simulation%",
        "%topological%insulator%quantum simulat%",
        "%topological%invariant%computation%",
        "%topological%invariant%quantum comput%",
        "%symmetry-protected%topological%",
        "%topological%phase%quantum simulat%",
    ],
    "FeMo-cofactor (nitrogenase) electronic structure": [
        "%nitrogenase%quantum%",
        "%FeMo%cofactor%",
        "%FeMoco%quantum%",
        "%nitrogen%fixation%quantum%comput%",
    ],

    # -- Quantum Simulation --
    "Time evolution of quantum spin systems": [
        # Tightened: drop bare "quantum spin chain"/"quantum spin simulat" which match generic spin physics;
        # require time-evolution/Trotter context
        "%hamiltonian simulation%",
        "%trotter%suzuki%",
        "%product formula%quantum%",
        "%time evolution%quantum%spin%",
        "%real-time%simulation%quantum%spin%",
        "%digital quantum simulation%spin%",
        "%digital%quantum%simulat%hamilton%",
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
        "%quantum%credit risk%",
        "%quantum%fraud%",
        "%quantum%credit%scor%",
    ],
    "Exotic derivative pricing (path-dependent options)": [
        "%exotic%derivative%quantum%",
        "%path%dependent%quantum%",
        "%asian%option%quantum%",
        "%barrier%option%quantum%",
    ],

    # -- Machine Learning --
    "Quantum kernel methods / QSVM": [
        "%quantum kernel%",
        "%QSVM%",
        "%quantum%support vector%",
        "%quantum%feature map%",
        "%kernel%quantum%circuit%",
        "%projected quantum kernel%",
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
        "%qPCA%",
        "%quantum dimensionality reduct%",
        "%density matrix exponentiation%",
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
        "%quantum%knapsack%",
        "%knapsack%QAOA%",
        "%knapsack%quantum anneal%",
        "%bin packing%QAOA%",
    ],
    "Maximum independent set": [
        "%maximum independent set%",
        "%independent set%rydberg%",
        "%independent set%QAOA%",
        "%independent set%quantum anneal%",
        "%independent set%quantum optim%",
        "%MIS%QAOA%",
        "%independent set%quantum%",
        "%quantum%independent set%",
    ],
    "Set cover and minimum vertex cover": [
        "%set cover%quantum%",
        "%vertex cover%quantum%",
        "%quantum%set cover%",
        "%quantum%vertex cover%",
        "%set cover%QAOA%",
        "%vertex cover%QAOA%",
        "%minimum vertex cover%",
    ],
    "Maximum flow and minimum cut": [
        # NB: avoid patterns like %quantum%max%flow% — they false-match
        # "quantum gas in maximum flow conditions" etc. Require the algorithmic
        # phrase as a unit.
        "%maximum flow%quantum%",
        "%minimum cut%quantum%",
        "%max-flow%quantum%",
        "%min-cut%quantum%",
        "%quantum%maximum flow%",
        "%quantum%minimum cut%",
        "%quantum%max-flow%",
        "%quantum%min-cut%",
        "%network flow%quantum algorithm%",
    ],
    "Gradient estimation and optimisation": [
        "%quantum%gradient%estimation%",
        "%jordan%gradient%quantum%",
        "%quantum%gradient%descent%",
        "%gradient%quantum%algorithm%",
        "%parameter shift%quantum%",
        "%quantum%natural gradient%",
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
        # Tightened: drop bare "quantum magnet simulat" (matches generic spintronics);
        # require explicit quantum-magnetism / spin-liquid / frustration concepts
        "%quantum magnetism%",
        "%spin liquid%",
        "%frustrated magnet%",
        "%kitaev%model%",
        "%quantum%antiferromagnet%simulat%",
        "%dimerised%magnet%quantum%",
        "%kagome%quantum%",
        "%pyrochlore%quantum%",
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
        "%anomaly detect%quantum%",
        "%quantum%autoencoder%",
        "%quantum%outlier%",
        "%quantum%novelty detect%",
    ],

    # -- Tranche 2: Finance (expanded) --
    "Quantum-enhanced market making and execution": [
        "%market making%quantum%",
        "%trade execution%quantum%",
        "%quantum%trading%",
        "%quantum%market making%",
        "%algorithmic trading%quantum%",
    ],
    "Financial clearing and netting optimisation": [
        "%clearing%quantum%",
        "%netting%quantum%",
        "%settlement%quantum%",
        "%quantum%clearing%",
        "%quantum%netting%",
        "%payment%netting%quantum%",
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
        "%quantum%flight%schedul%",
        "%airport%quantum%optim%",
        "%aviation%quantum%optim%",
    ],
    "Warehouse picking and layout optimisation": [
        "%warehouse%quantum%",
        "%picking%optim%quantum%",
        "%quantum%warehouse%",
        "%order picking%quantum%",
        "%warehouse layout%quantum%",
        "%logistics warehouse%quantum%",
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
        # Tightened: bare "quantum network" matches all neural-net / generic networking papers;
        # require routing/distribution/internet/repeater context
        "%quantum%network%routing%",
        "%quantum internet%",
        "%entanglement distribution%",
        "%quantum repeater%",
        "%quantum%key%distribution%network%",
        "%quantum communication network%",
        "%entanglement%routing%",
        "%QKD%network%",
    ],

    # -- Tranche 3: Optimisation (more niche) --
    "Minimum spanning tree and Steiner tree": [
        "%spanning tree%quantum%",
        "%steiner tree%quantum%",
        "%quantum%spanning tree%",
        "%quantum%steiner%",
        "%minimum spanning%QAOA%",
    ],
    "Facility location and p-median": [
        "%facility location%quantum%",
        "%p-median%quantum%",
        "%quantum%facility location%",
        "%facility location%QAOA%",
        "%facility location%quantum anneal%",
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
        "%transfer learning%quantum%",
        "%hybrid%classical%quantum%neural%",
        "%pretrain%quantum%neural%",
        "%fine%tun%quantum%circuit%",
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


# ---------------------------------------------------------------------------
# German keyword variants.
#
# DFG GEPRIS (152K records) and BMBF Förderkatalog (268K records) are mostly
# German. Without German patterns, these 420K records contribute almost
# nothing to QA matching. Patterns merged into APPLICATION_KEYWORDS at import.
#
# Naming conventions:
#   quantum         → quanten (Quantencomputing, Quantenchemie)
#   simulation      → simulation (loanword, identical)
#   molecular       → molekül / molekular
#   electronic      → elektronisch / elektronen
#   ground state    → grundzustand
#   superconductor  → supraleiter / supraleitung
#   algorithm       → algorithmus / algorithmen
#   optimization    → optimierung
#   machine learning → maschinelles lernen / maschinenlernen
#   topological     → topologisch
#   many-body       → vielteilchen
#   lattice         → gitter
# ---------------------------------------------------------------------------

GERMAN_VARIANTS: dict[str, list[str]] = {
    # -- Cryptography --
    "Integer factorisation": [
        "%faktorisierung%quanten%",
        "%shor%algorithmus%",
        "%post%quanten%kryptograph%",
        "%post%quanten%kryptografie%",
    ],
    "Discrete logarithm over finite fields": [
        "%diskrete%logarithm%quanten%",
    ],
    "Symmetric key search": [
        "%grover%suche%",
        "%grover%algorithmus%",
    ],
    "Lattice problem solving (LWE/SVP)": [
        "%gitter%kryptograph%",
        "%gitter%basierte%krypto%",
    ],
    "Quantum random number generation": [
        "%quanten%zufallszahl%",
        "%quanten%zufallsgenerator%",
    ],

    # -- Chemistry --
    "Molecular ground state energy estimation": [
        "%molek%grundzustand%quanten%",
        "%quantenchemie%",
        "%elektronische%struktur%quanten%",
        "%variations%quanten%eigensolver%",
        "%quantenalgorithmus%chemie%",
    ],
    "Molecular excited state computation": [
        "%angeregte%zustand%quanten%",
        "%angeregter%zustand%quanten%",
    ],
    "Catalytic reaction mechanism elucidation": [
        "%katalys%quanten%",
        "%reaktions%mechanism%quanten%",
        "%übergangs%zustand%quanten%",
    ],
    "Vibrational structure and molecular spectra": [
        "%schwingungs%quanten%simulat%",
        "%molekülspektr%quanten%",
        "%vibronische%quanten%",
    ],
    "Photosynthetic energy transfer simulation": [
        "%photosynthes%quanten%",
        "%lichtsammel%quanten%",
    ],
    "Open-shell and radical species computation": [
        "%offene%schale%quanten%",
        "%multireferenz%quanten%",
        "%radikal%quanten%",
    ],

    # -- Materials Science --
    "High-Tc superconductor simulation": [
        "%hochtemperatur%supraleit%",
        "%hoch%temperatur%supraleit%",
        "%supraleiter%simulat%",
        "%hubbard%modell%quanten%",
    ],
    "Topological phase classification": [
        "%topologische%phase%quanten%",
        "%topologische%isolator%quanten%",
        "%topologische%materie%",
        "%topologischer%isolator%quanten%",
    ],
    "Battery electrolyte and electrode simulation": [
        "%batterie%quanten%",
        "%elektrolyt%quanten%",
        "%lithium%quanten%simulat%",
    ],
    "Photovoltaic material simulation": [
        "%photovoltaik%quanten%",
        "%solarzelle%quanten%",
        "%perowskit%quanten%",
    ],
    "Semiconductor defect simulation": [
        "%defekt%quanten%comput%",
        "%halbleiter%defekt%quanten%",
        "%punktdefekt%quanten%",
        "%NV%zentr%quanten%",
        "%stickstoff%fehlstelle%",
    ],

    # -- Quantum Simulation --
    "Time evolution of quantum spin systems": [
        "%hamilton%simulation%",
        "%spinsystem%quanten%",
        "%quanten%spinkette%",
        "%quanten%spinmodell%",
    ],
    "Quantum field theory simulation": [
        "%quantenfeld%theorie%simulat%",
        "%gittereich%quanten%",
        "%quantenchromodynamik%simulat%",
    ],
    "Ground state preparation of local Hamiltonians": [
        "%grundzustand%präparat%quanten%",
        "%adiabatische%zustandspräparat%",
    ],
    "Lindbladian dynamics simulation": [
        "%lindblad%quanten%",
        "%offene%quantensystem%",
        "%dissipative%quanten%simulat%",
    ],
    "Quantum magnetism simulation": [
        "%quantenmagnetismus%",
        "%quanten%magnet%simulat%",
        "%spinflüssigkeit%quanten%",
        "%frustrierte%magnet%quanten%",
    ],
    "Bose-Hubbard model simulation": [
        "%bose%hubbard%",
        "%suprafluid%mott%",
    ],
    "Phonon and electron-phonon coupling simulation": [
        "%elektron%phonon%quanten%",
        "%polaron%quanten%",
    ],
    "Quantum quench dynamics": [
        "%quantenquench%",
        "%nichtgleichgewicht%quanten%simulat%",
    ],
    "Thermalisation and many-body localisation": [
        "%vielteilchen%lokalisier%",
        "%thermalisier%quanten%",
    ],
    "Quantum Hall effect simulation": [
        "%quanten%hall%simulat%",
        "%fraktionaler%quanten%hall%",
        "%anyon%quanten%",
    ],
    "Quantum gravity and cosmology simulation": [
        "%quantengravitat%simulat%",
        "%schleifen%quantengravitat%",
        "%holograph%quanten%",
    ],
    "Parton shower and scattering simulation": [
        "%parton%quanten%",
        "%streu%quanten%simulat%",
        "%hochenergiephysik%quantencomput%",
    ],
    "Nuclear structure calculation": [
        "%kern%struktur%quantencomput%",
        "%kern%simulat%quanten%",
        "%atomkern%quanten%",
    ],

    # -- Quantum Computing Infrastructure --
    "Quantum error correction decoding": [
        "%quantenfehlerkorrektur%",
        "%quanten%fehler%korrektur%",
        "%fehler%toleranter%quanten%",
        "%oberflächen%code%quanten%",
        "%topologischer%code%",
    ],
    "Quantum circuit optimisation and compilation": [
        "%quantenschaltkreis%optim%",
        "%quanten%compiler%",
        "%gattersynthese%quanten%",
    ],
    "Quantum volume and fidelity benchmarking": [
        "%quantenvolumen%",
        "%quanten%benchmark%",
        "%randomisier%benchmark%",
    ],
    "Quantum error mitigation": [
        "%fehlerminderung%quanten%",
        "%fehler%mitigat%quanten%",
        "%null%rausch%extrapol%",
    ],
    "Quantum state tomography and verification": [
        "%quantentomograph%",
        "%klassische%schatten%",
        "%fidelity%schätz%quanten%",
    ],
    "Quantum network routing and entanglement distribution": [
        "%quantennetz%",
        "%quanteninternet%",
        "%verschränkungs%verteil%",
        "%quantenrepeater%",
    ],

    # -- Optimisation --
    "Max-Cut and graph partitioning": [
        "%max%cut%quanten%",
        "%graph%partition%quanten%",
        "%QAOA%",
    ],
    "Boolean satisfiability (SAT)": [
        "%erfüllbarkeit%quanten%",
        "%boolesche%quanten%",
    ],
    "Travelling salesman and vehicle routing": [
        "%handlungsreisend%quanten%",
        "%fahrzeug%routing%quanten%",
    ],
    "Convex optimisation / SDP solving": [
        "%semidefinit%quanten%",
        "%konvex%optim%quanten%",
    ],
    "Job-shop scheduling": [
        "%ablauf%planung%quanten%",
        "%jobshop%quanten%",
    ],
    "Quadratic unconstrained binary optimisation (QUBO)": [
        "%ising%formulier%quanten%",
        "%quanten%annealing%optim%",
    ],

    # -- Machine Learning --
    "Quantum neural networks / variational classifiers": [
        "%quanten%neuronal%",
        "%variations%klassifikator%quanten%",
        "%parametrisierter%quantenschaltkreis%",
    ],
    "Quantum kernel methods / QSVM": [
        "%quantenkern%method%",
        "%quanten%support%vektor%",
    ],
    "Quantum-enhanced reinforcement learning": [
        "%quanten%verstärkung%lern%",
        "%quanten%bestärk%lern%",
    ],

    # -- Linear Algebra --
    "Solving sparse linear systems (HHL)": [
        "%quanten%lineare%system%",
        "%quanten%lineare%gleichung%",
    ],
    "Quantum ODE/PDE solvers": [
        "%quanten%differential%gleichung%",
        "%quanten%partielle%differential%",
    ],
    "Quantum phase estimation": [
        "%phasen%schätz%quanten%",
        "%eigenwert%quanten%algorithm%",
    ],

    # -- Finance --
    "Monte Carlo option pricing": [
        "%options%bewert%quanten%",
        "%quanten%monte%carlo%finanz%",
        "%derivat%bewertung%quanten%",
    ],
    "Portfolio optimisation": [
        "%portfolio%optim%quanten%",
        "%vermögens%allokat%quanten%",
        "%markowitz%quanten%",
    ],

    # -- Mathematics --
    "Period finding and hidden subgroup problem": [
        "%verborgene%untergruppe%",
        "%verborgenes%untergruppen%",
        "%quanten%fourier%transform%",
        "%periodenfind%quanten%",
    ],
    "Quantum algorithm for Jones polynomial": [
        "%jones%polynom%",
        "%knoten%invariant%quanten%",
    ],

    # -- Energy --
    "Plasma confinement simulation": [
        "%plasma%quantencomput%",
        "%fusion%quantencomput%",
        "%tokamak%quantencomput%",
    ],
    "Power grid unit commitment": [
        "%stromnetz%quanten%",
        "%energie%netz%quanten%",
        "%netz%dispatch%quanten%",
    ],

    # -- Engineering --
    "CFD via quantum linear algebra": [
        "%strömungs%dynamik%quanten%",
        "%navier%stokes%quanten%",
    ],
    "Finite element analysis via quantum linear solvers": [
        "%finite%elemente%quanten%",
        "%struktur%mechanik%quanten%",
    ],

    # -- Earth Science --
    "Climate and weather simulation": [
        "%klima%quantencomput%",
        "%wetter%quantencomput%",
        "%atmosph%quantensim%",
    ],

    # -- Defence --
    "Quantum radar and target detection": [
        "%quantenradar%",
        "%quanten%illumination%",
    ],
    "Quantum-enhanced GPS-denied navigation": [
        "%quanten%navigation%",
        "%quanten%inertial%",
        "%atom%interferom%navigat%",
    ],

    # -- Life Sciences --
    "Protein folding energy landscape exploration": [
        "%proteinfaltung%quanten%",
        "%protein%konformation%quanten%",
        "%protein%struktur%quantencomput%",
    ],
    "Sequence alignment and genomic analysis": [
        "%sequenz%alignment%quanten%",
        "%genom%quantencomput%",
    ],
}

# Merge German variants into main keyword dict
for _app, _de_patterns in GERMAN_VARIANTS.items():
    if _app in APPLICATION_KEYWORDS:
        APPLICATION_KEYWORDS[_app] = APPLICATION_KEYWORDS[_app] + _de_patterns
    else:
        # New application not in main dict — add it
        APPLICATION_KEYWORDS[_app] = _de_patterns


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


def _all_patterns_union() -> list[str]:
    """Flat list of all distinct ILIKE patterns across all applications.

    Used to pre-filter the 4M-row table down to ~50K candidates in one pass.
    """
    seen: set[str] = set()
    out: list[str] = []
    for patterns in APPLICATION_KEYWORDS.values():
        for p in patterns:
            if p not in seen:
                seen.add(p)
                out.append(p)
    return out


def _build_candidate_table(fs_conn: duckdb.DuckDBPyConnection) -> int:
    """Pre-filter grant_award_deduped to grants matching ANY app pattern.

    Single full-table scan with one giant OR clause. Reduces 4M rows to
    a small candidate set (~30-100K) that all per-app queries then run
    against. Cuts total runtime from ~16min to ~30s.

    Returns the number of candidate rows.
    """
    patterns = _all_patterns_union()
    # Build the global OR clause matching title or abstract
    conds = []
    for p in patterns:
        safe = p.replace("'", "''")
        conds.append(f"project_title ILIKE '{safe}'")
        conds.append(f"abstract ILIKE '{safe}'")
    where = " OR ".join(conds)

    fs_conn.execute("DROP TABLE IF EXISTS _qa_candidates")
    fs_conn.execute(f"""
        CREATE TEMP TABLE _qa_candidates AS
        SELECT id, project_title, abstract,
               total_funding_eur, total_funding_estimated, funder_id
        FROM grant_award_deduped
        WHERE {where}
    """)
    return fs_conn.execute("SELECT COUNT(*) FROM _qa_candidates").fetchone()[0]


def compute_funding_links(
    fundingscape_path: str | None = None,
    qa_path: str | None = None,
    verbose: bool = False,
) -> dict[str, dict]:
    """Query fundingscape for each application and store TAM results.

    Returns a dict of {app_name: {grant_count, total_funding_eur, top_funders}}.

    Performance: pre-filters the full grant table once into a small candidate
    table, then runs per-app queries against that. ~30s vs ~16min naive.
    """
    fs_path = fundingscape_path or DB_PATH
    fs_conn = duckdb.connect(fs_path, read_only=True)
    qa_conn = get_qa_connection(qa_path)

    if verbose:
        print("Building candidate table (one full table scan)...")
    n_candidates = _build_candidate_table(fs_conn)
    if verbose:
        print(f"  → {n_candidates:,} candidate grants")

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

        # Count + sum funding + top funders, all from the small candidate table.
        # Single CTE so only one scan of _qa_candidates per app.
        result = fs_conn.execute(f"""
            WITH matched AS (
                SELECT total_funding_eur, total_funding_estimated, funder_id
                FROM _qa_candidates
                WHERE {where}
            ),
            funder_counts AS (
                SELECT funder_id, COUNT(*) c FROM matched GROUP BY funder_id
            ),
            top_funders AS (
                SELECT string_agg(
                    COALESCE(f.short_name, 'Unknown') || '(' || fc.c || ')',
                    ', '
                    ORDER BY fc.c DESC
                ) AS top_str
                FROM (SELECT funder_id, c FROM funder_counts ORDER BY c DESC LIMIT 5) fc
                LEFT JOIN funder f ON fc.funder_id = f.id
            )
            SELECT
                (SELECT COUNT(*) FROM matched) AS cnt,
                (SELECT COALESCE(SUM(COALESCE(total_funding_eur, total_funding_estimated, 0)), 0)
                 FROM matched) AS funding,
                (SELECT top_str FROM top_funders) AS top
        """).fetchone()

        grant_count = result[0]
        total_funding = result[1]
        top_funders = result[2] or ""

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
