;;;; pubnames.lisp
;;;
;;; We get data in XML format, parse it and push it to a PostgreSQL table.
;;;

(in-package #:pubnames)

(defparameter *pub-xml-pathname*
  (asdf:system-relative-pathname :pubnames "pub.xml"))

(defparameter *pgconn* '("dim" "dim" "none" "localhost" :port 54393)
  "PostgreSQL Connection String")

(defparameter *pg-table-name* "pubnames" "PostgreSQL table to use.")

(defparameter *pg-create-table*
  (format nil "create table if not exists ~a
                (id bigint, pos point, name text)" *pg-table-name*)
  "PostgreSQL query to create the table")

(defvar *current-osm* nil "Current NODE being parsed")
(defvar *current-tag* nil "Current TAG node being parsed")

(defstruct osm
  id lat lon
  amenity created_by name phone postal_code)

(defun osm-parse-node-attr (ns name fqdn value s-p)
  "Maybe add the current <node> attribute to the O osm struct"
  (declare (ignore ns fqdn s-p))
  (when (member name '("id" "lat" "lon") :test #'string=)
    (let ((slot-name (intern (string-upcase name) :pubnames)))
      (setf (slot-value *current-osm* slot-name) value))))

(defun osm-parse-tag-attr (ns name fqdn value s-p)
  "Maybe add the current <tag> attribute to the osm struct"
  (declare (ignore ns fqdn s-p))
  (cond
    ((string= name "k")
     (let ((k (intern (string-upcase value) :pubnames)))
       (when (slot-exists-p *current-osm* k)
	 (setf (car *current-tag*) k))))

    ((string= name "v")
     (setf (cdr *current-tag*) value))))

(defmethod parse-osm-tag (s tag) "Ignore other tags")

(defmethod parse-osm-tag (s (tag (eql 'node)))
  "Parse a <node> tag"
  (setf *current-osm* (make-osm))
  (klacks:map-attributes #'osm-parse-node-attr s))

(defmethod parse-osm-tag (s (tag (eql 'tag)))
  "Parse a <tag> tag"
  (setf *current-tag* (cons nil nil))
  (klacks:map-attributes #'osm-parse-tag-attr s)
  (when (car *current-tag*)
    (setf (slot-value *current-osm* (car *current-tag*))
	  (cdr *current-tag*))))

(defun current-qname-as-symbol (source)
  "Return current SOURCE tag name as an interned symbol in PUBNAME package."
  (let* ((tag-name (string-upcase (klacks:current-qname source))))
    (find-symbol tag-name :pubnames)))

(defun parse-osm-start-element (source)
  "Route parsing depending on the source tag name."
  (parse-osm-tag source (current-qname-as-symbol source)))

(defun parse-osm-end-element (source stream)
  "When we're done with a <node>, send the data over to the stream"
  (when (and (eq 'node (current-qname-as-symbol source))
	     *current-osm*)
    ;; don't send data if we don't have a pub name
    (when (osm-name *current-osm*)
      (cl-postgres:db-write-row stream (osm-to-pgsql *current-osm*)))

    ;; reset *current-osm* for parsing the next <node>
    (setf *current-osm* nil)))

(defmethod osm-to-pgsql ((o osm))
  "Convert an OSM struct to a list that we can send over to PostgreSQL"
  (list (osm-id o)
	(format nil "(~a,~a)" (osm-lat o) (osm-lon o))
	(osm-name o)))

(defun maybe-create-postgresql-table (&key drop truncate)
  "If our *pg-table-name* does not exists, create it"
  (with-connection *pgconn*
    (when drop
      (execute (format nil "drop table if exists ~a;" *pg-table-name*)))

    (execute *pg-create-table*)

    (when truncate
      (execute (format nil "truncate ~a;" *pg-table-name*)))))

(defun parse-osm-file (&key
			 (pathname *pub-xml-pathname*)
			 (truncate t)
			 (drop nil))
  "Parse the given PATHNAME file, formated as OSM XML."
  (maybe-create-postgresql-table :drop drop :truncate truncate)
  (klacks:with-open-source (s (cxml:make-source pathname))
    (loop
       with stream =
	 (cl-postgres:open-db-writer (remove :port *pgconn*) *pg-table-name* nil)
       for key = (klacks:peek s)
       while key
       do
	 (case key
	   (:start-element (parse-osm-start-element s))
	   (:end-element   (parse-osm-end-element s stream)))
	 (klacks:consume s)

       finally (return (cl-postgres:close-db-writer stream)))))

