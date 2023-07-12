;;; ds-spy-util.el --- Description -*- lexical-binding: t; -*-
;;
;;
;; This file is not part of GNU Emacs.
;;
;;; Commentary:
;;
;;  Description:
;;
;;; Code:


;; user config at the end of the file.

(require 'cider)

(defvar ds-spy-util-run-form-ns nil
  "The ns in which `ds-spy-util-run-form' will run.")
(defvar ds-spy-util-run-form nil
  "Code that will run a pipeline that includes the transform you want to spy in.
It should be a blocking call, ie include a `wait-pipeline-result'.")

(defvar ds-spy-util-dir-no-slash (concat (getenv "HOME") "/ds-spy")
  "Has to exist, I didn't bother to check and mkdir it if not.")

(defconst ds-spy-util-clj-template
  ;; TODO: handle views as well
  ;; now that we put that in the same repo as repl-utils that should be easy
  "
      ((fn [pcoll]
         (ds/write-edn-file \"%s\" {:num-shards 1} pcoll) pcoll))")


(defun ds-spy-util--sync-eval-defun ()
  (cider-nrepl-sync-request:eval (cider-defun-at-point)
                                 nil
                                 (cider-current-ns)))

(defun ds-spy-util ()
  "Automates spying inside a pipeline.
Before invoking this command, you should place point inside a `ds/->>',
after the form the output of which you want to spy, and
make sure a repl is open and `ds-spy-util-run-form'
and `ds-spy-util-run-form-ns' are set,
and that that ns has been evaluated."
  (interactive)
  (let* ((output-name-read (read-string "optionally name output file (default \"last.edn\"): " nil nil "last"))
         (output-name-final (format "%s/%s.edn"
                              ds-spy-util-dir-no-slash
                              output-name-read))
         (output-name-tmp (concat output-name-final ".tmp"))
         (output-name-tmp-beam (concat output-name-tmp "-00000-of-00001"))
         (ins (format ds-spy-util-clj-template
                      output-name-tmp))
         (buf-was-clean-p (not (buffer-modified-p))))
    (insert ins)
    (let* ((response
            (ds-spy-util--sync-eval-defun))
           (response-string (format "%s" response)))
      (when (string-match-p "error" response-string)
        (delete-char (- (length ins)))
        (error "ds-spy-util error during first defun eval : %s" response-string)))

    (let* ((response
            (cider-nrepl-sync-request:eval ds-spy-util-run-form nil ds-spy-util-run-form-ns) )
           (response-string (format "%s" response)))
      (delete-char (- (length ins)))
      (when buf-was-clean-p
        (set-buffer-modified-p nil))
      (cond
       ((string-match-p "namespace-not-found" response-string)
        (error "ds-spy-util error : ns error, make sure it has been evaluated"))
       ((string-match-p "error" response-string)
        (error "ds-spy-util error, maybe this helps : %s" response-string))
       ((not (file-exists-p output-name-tmp-beam))
        (error "ds-spy-util error : didn't create file apparently"))))
    (ds-spy-util--sync-eval-defun)
    (let ((jet (executable-find "jet")))
      ;; there is `cider-format-edn-buffer', but the file
      ;; needs to be linked to the repl and I did'nt know
      ;; how to make it work.
      (when (not jet) (error "please install jet for pretty printing"))
      (call-process-shell-command (format "cat %s | jet > %s"
                                          output-name-tmp-beam
                                          output-name-final))
      (delete-file output-name-tmp-beam)
      (find-file-other-window output-name-final))))

;; user config

;; (setq ds-spy-util-dir-no-slash "")
;; (setq ds-spy-util-run-form "")

(provide 'ds-spy-util)
